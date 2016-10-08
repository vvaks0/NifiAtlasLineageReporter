package com.hortonworks.nifi;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.jersey.api.client.WebResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

@Tags({"reporting", "atlas", "lineage", "governance"})
@CapabilityDescription("Publishes flow changes and metadata to Apache Atlas")
public class AtlasLineageReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor ATLAS_URL = new PropertyDescriptor.Builder()
            .name("Atlas URL")
            .description("The URL of the Atlas Server")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("http://localhost:21000")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    static final PropertyDescriptor ACTION_PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("Action Page Size")
            .description("The size of each page to use when paging through the NiFi actions list.")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private long lastId = 0; // TODO store on disk, this is for demo only
    private AtlasClient atlasClient;
    private Map<String, Referenceable> nifiFlowIngressMap = new HashMap<String,Referenceable>();
    private Map<String, Referenceable> nifiFlowEgressMap = new HashMap<String,Referenceable>();
    private List<String> nifiLineage = new ArrayList<String>();
    private List<String> nifiFlowFiles = new ArrayList<String>();
    private Referenceable outgoingEvent = null;
    private Referenceable incomingEvent = null;
    private int timesTriggered = 0;
    private WebResource service;
    private String atlasUrl = "http://sandbox.hortonworks.com:21000";
    public static final String DEFAULT_ADMIN_USER = "admin";
	public static final String DEFAULT_ADMIN_PASS = "admin";
    private Double atlasVersion;
    private final static Map<String, HierarchicalTypeDefinition<ClassType>> classTypeDefinitions = new HashMap();
	private final static Map<String, EnumTypeDefinition> enumTypeDefinitionMap = new HashMap();
	private final static Map<String, StructTypeDefinition> structTypeDefinitionMap = new HashMap();
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATLAS_URL);
        properties.add(ACTION_PAGE_SIZE);
        return properties;
    }
    
    @Override
    public void onTrigger(ReportingContext reportingContext) {
        // create the Atlas client if we don't have one
    	Properties props = System.getProperties();
        props.setProperty("atlas.conf", "/usr/hdp/current/atlas-server/conf");
        getLogger().info("***************** atlas.conf has been set to: " + props.getProperty("atlas.conf"));
    	
    	String[] basicAuth = {DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASS};
		String[] atlasURL = {atlasUrl};
    	if (atlasClient == null) {
            atlasUrl = reportingContext.getProperty(ATLAS_URL).getValue();
            getLogger().info("Creating new Atlas client for {}", new Object[] {atlasUrl});
            atlasClient = new AtlasClient(atlasURL, basicAuth);
        }

        atlasVersion = Double.valueOf(getAtlasVersion(atlasUrl + "/api/atlas/admin/version", basicAuth));
        getLogger().info("********************* Atlas Version is: " + atlasVersion);
		
        generateAvroSchemaDataModel();
        
        final EventAccess eventAccess = reportingContext.getEventAccess();
        final int pageSize = reportingContext.getProperty(ACTION_PAGE_SIZE).asInteger();
        
        //When the Task triggers for the first time, find the last eventId and start from there 
        List<ProvenanceEventRecord> events = new ArrayList<ProvenanceEventRecord>();
        if(timesTriggered==0){
        	while (events != null && events.size() > 0) {
        		try {
        			events = eventAccess.getProvenanceEvents(lastId, pageSize);
        		} catch (IOException e) {
        			e.printStackTrace();
        		}
        		lastId = events.get(events.size()-1).getEventId();
        	}	
        	events.clear();
        }
        
        // grab new actions starting from lastId up to pageSize
        try {
			events = eventAccess.getProvenanceEvents(lastId, pageSize);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
        if (events == null || events.size() == 0) {
            getLogger().info("No actions to process since last execution, lastId = {}", new Object[] {lastId});
        }

        // page through actions and process each page
        while (events != null && events.size() > 0) {
            for (final ProvenanceEventRecord event : events) {
                try {
                    // TODO eventually send multiple actions in a single request
                    processEvent(event);
                } catch (Exception e) {
                    getLogger().error("Unable to process event", e);
                    throw new ProcessException(e);
                }
                lastId = event.getEventId();
            }
            
            try {
				events = eventAccess.getProvenanceEvents(lastId+1, pageSize);
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        String currentEvent;
        Iterator<String> eventsIterator = nifiFlowFiles.iterator();
        while(eventsIterator.hasNext()){
        	currentEvent = eventsIterator.next();
        	if(nifiFlowIngressMap.containsKey(currentEvent) && nifiFlowEgressMap.containsKey(currentEvent)){
        		Referenceable nifiFlowRef = createNifiFlow(reportingContext, nifiFlowIngressMap.get(currentEvent), nifiFlowEgressMap.get(currentEvent));
        		try {
					register(atlasClient, nifiFlowRef);
				} catch (Exception e) {
					e.printStackTrace();
				}
        		getLogger().info("Created new flow: " 
        				+ nifiFlowRef.toString()
        				+ " for flow file: " + currentEvent);
        		
        	}else{
        		getLogger().info("Current flow file is missing an Ingress or Egress point: " + currentEvent);
        	}
        }
        getLogger().info("Done processing actions");
        nifiFlowFiles.clear();
        nifiFlowIngressMap.clear();
        nifiFlowEgressMap.clear();
        nifiLineage.clear();
        timesTriggered++;
    }
    
    private void processEvent(ProvenanceEventRecord event) throws Exception {
        getLogger().info("Processing event with id {}", new Object[] {event.getEventId()});
        //ReferenceableUtil.register(atlasClient, createProcessor(action));
        String eventType = event.getEventType().name();
        getLogger().info("Processing event of type {}", new Object[] {eventType});
        String generatedUuid = UUID.randomUUID().toString();
        if(eventType.equalsIgnoreCase("RECEIVE")){
        	try {
				incomingEvent = getEventReference(event, generatedUuid);
			} catch (Exception e) {
				getLogger().debug("******************** CAUGHT EXCEPTION" + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));
			}
        	if(incomingEvent != null){
				getLogger().info("********************* Source Referenceable Event: " + incomingEvent.toString());
				nifiFlowIngressMap.put(event.getFlowFileUuid(), incomingEvent);
			}else{
				getLogger().info("********************* Could not find Referenceable Event, creating...." + generatedUuid );
				nifiFlowIngressMap.put(event.getFlowFileUuid(), register(atlasClient, createEvent(event, generatedUuid)));
			}
        	nifiFlowFiles.add(event.getFlowFileUuid());
        	nifiLineage.add(event.getComponentType() + ":" + eventType);
        }else if(eventType.equalsIgnoreCase("SEND")){
        	try {
				outgoingEvent = getEventReference(event);
			} catch (Exception e) {
				e.printStackTrace();
				getLogger().debug("******************** CAUGHT EXCEPTION" + e.getMessage() + " : " + Arrays.toString(e.getStackTrace()));
			}
			if(outgoingEvent != null){
				getLogger().info("********************* Source Referenceable Event: " + outgoingEvent.toString());
				nifiFlowEgressMap.put(event.getFlowFileUuid(), outgoingEvent);
			}else{
				getLogger().info("********************* Could not find Referenceable Event, creating...." + event.getFlowFileUuid());
				nifiFlowEgressMap.put(event.getFlowFileUuid(), register(atlasClient, createEvent(event, generatedUuid)));
			}        	
        	nifiLineage.add(event.getComponentType() + ":" + eventType);
        }else{
        	getLogger().info("Event type is: " + eventType + ", adding to actions list....");
        	nifiLineage.add(event.getComponentType() + ":" + eventType);
        }
    }
    /*
    private void processEvent(ProvenanceEventRecord event) throws Exception {
        getLogger().info("Processing event with id {}", new Object[] {event.getEventId()});
        //ReferenceableUtil.register(atlasClient, createProcessor(action));
        String eventType = event.getEventType().name();
        getLogger().info("Processing event iof type {}", new Object[] {eventType});
        if(eventType.equalsIgnoreCase("RECEIVE")){
        	nifiFlowIngressMap.put(event.getFlowFileUuid(), register(atlasClient, createIngressProcessor(event)));
        	nifiFlowFiles.add(event.getFlowFileUuid());
        }else if(eventType.equalsIgnoreCase("SEND")){
        	nifiFlowEgressMap.put(event.getFlowFileUuid(), register(atlasClient, createEgressProcessor(event)));        	
        }else{
        	getLogger().info("Event type is: " + eventType + ", skipping....");
        }
    }*/
    
	public Referenceable register(final AtlasClient atlasClient, final Referenceable referenceable) throws Exception {
        if (referenceable == null) {
            return null;
        }

        final String typeName = referenceable.getTypeName();
        getLogger().info("creating instance of type " + typeName);

        //final String entityJSON = InstanceSerialization.toJson(referenceable, true);
        //getLogger().info("Submitting new entity {} = {}", new Object[] {referenceable.getTypeName(), entityJSON});
        getLogger().info("Submitting new entity: " + referenceable.toString());
        //final List<String> guid = atlasClient.createEntity(entityJSON);
        final List<String> guid = atlasClient.createEntity(referenceable);
        getLogger().info("created instance for type " + typeName + ", guid: " + guid);

        return new Referenceable(guid.get(guid.size() - 1) , referenceable.getTypeName(), null);
    }

    private Referenceable createNifiFlow(final ReportingContext context, final Referenceable ingressPoint, final Referenceable egressPoint) {
        final String id = context.getEventAccess().getControllerStatus().getId();
        final String name = context.getEventAccess().getControllerStatus().getName();
        final String jsonClass = "org.apache.atlas.typesystem.json.InstanceSerialization$_Id";
        final Integer version = 0;
        final String typeName = "DataSet";
        final LineageReferenceType[] inputs = {new LineageReferenceType(ingressPoint.getId()._getId().replace("[", "").replace("]", "").replace("\"", "").replace("\\", ""), jsonClass, version, typeName)};
        final LineageReferenceType[] outputs = {new LineageReferenceType(egressPoint.getId()._getId().replace("[", "").replace("]", "").replace("\"", "").replace("\\", ""), jsonClass, version, typeName)};
        
        final Referenceable nifiFlow = new Referenceable("nifi_flow");
        nifiFlow.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, id);
        nifiFlow.set("flow_id", id);
        nifiFlow.set("name", name+"_"+id+"_"+inputs[0].getId()+"_"+outputs[0].getId());
        nifiFlow.set("inputs", inputs);
        nifiFlow.set("outputs", outputs);
        nifiFlow.set("description", Arrays.toString(nifiLineage.toArray()));
        
        return nifiFlow;
    }

    //Use this version of method when Flow File UUID has been assigned
    private Referenceable createEvent(final ProvenanceEventRecord event) {
        final String flowFileUuid = event.getFlowFileUuid();
        
        // TODO populate processor properties and determine real parent group, assuming root group for now
        final Referenceable processor = new Referenceable("event");
        processor.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, flowFileUuid);
        processor.set("name", flowFileUuid);
        processor.set("event_key", "accountNumber");
        processor.set("description", flowFileUuid);
        return processor;
    }
    
    //Use this version of method when incoming event is ingested and Flow File UUID has not yet been assigned
    private Referenceable createEvent(final ProvenanceEventRecord event, final String uuid) {
        final Referenceable processor = new Referenceable("event");
        processor.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, uuid);
        processor.set("name", uuid);
        processor.set("event_key", "accountNumber");
        processor.set("description", uuid);
        return processor;
    }
    
    private Referenceable createIngressProcessor(final ProvenanceEventRecord event) {
        final String id = event.getComponentId();
        final String name = event.getComponentType();
        final String ingressURL = event.getTransitUri();
        final String flowFileUuid = event.getFlowFileUuid();
        
        // TODO populate processor properties and determine real parent group, assuming root group for now
        final Referenceable processor = new Referenceable("nifi_input_processor");
        processor.set("component_id", id);
        processor.set("name", name + "_" + flowFileUuid);
        processor.set("flow_file_uuid", flowFileUuid);
        processor.set("ingress_point", ingressURL);
        processor.set("description", name);
        return processor;
    }
    
    private Referenceable createEgressProcessor(final ProvenanceEventRecord event) {
        final String id = event.getComponentId();
        final String name = event.getComponentType();
        final String egressURL = event.getTransitUri();
        final String flowFileUuid = event.getFlowFileUuid();

        // TODO populate processor properties and determine real parent group, assuming root group for now
        final Referenceable processor = new Referenceable("nifi_output_processor");
        processor.set("component_id", id);
        processor.set("name", name + "_" + flowFileUuid);
        processor.set("flow_file_uuid", flowFileUuid);
        processor.set("egress_point", egressURL);
        processor.set("description", name);
        return processor;
    }
    
    private Referenceable getEventReference(ProvenanceEventRecord event) throws Exception {
		final String typeName = "event";
		final String id = event.getFlowFileUuid();
		 
		String dslQuery = String.format("%s where %s = \"%s\"", typeName, "name", id);
		getLogger().info("********************* Atlas Version is: " + atlasVersion);
		//Referenceable eventReferenceable = null;
		/*
		if(atlasVersion.equalsIgnoreCase("0.5"))
			return getEntityReferenceFromDSL5(atlasClient, typeName, dslQuery);
		else if(atlasVersion.equalsIgnoreCase("0.6"))
			return getEntityReferenceFromDSL6(atlasClient, typeName, dslQuery);
		else
			return null;
		*/
		
		if(atlasVersion >= 0.7)
			return getEntityReferenceFromDSL6(atlasClient, typeName, dslQuery);
		else
			return null;
	}
    // Used this when event uuid is coming from external system
    private Referenceable getEventReference(ProvenanceEventRecord event, String uuid) throws Exception {
		final String typeName = "event";
		
		String dslQuery = String.format("%s where %s = \"%s\"", typeName, "name", uuid);
		getLogger().info("********************* Atlas Version is: " + atlasVersion);
		//Referenceable eventReferenceable = null;
		/*
		if(atlasVersion.equalsIgnoreCase("0.5"))
			return getEntityReferenceFromDSL5(atlasClient, typeName, dslQuery);
		else if(atlasVersion.equalsIgnoreCase("0.6"))
			return getEntityReferenceFromDSL6(atlasClient, typeName, dslQuery);
		else
			return null;
		*/
		
		if(atlasVersion >= 0.7)
			return getEntityReferenceFromDSL6(atlasClient, typeName, dslQuery);
		else
			return null;
	}
    
    public static Map<String, String> getFieldValues(Object instance, boolean prependClassName) throws IllegalAccessException {
		Class clazz = instance.getClass();
		Map<String, String> output = new HashMap<>();
		
		for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
			Field[] fields = c.getDeclaredFields();
			for (Field field : fields) {
				if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
					continue;
				}	

				String key;
				if (prependClassName) {
					key = String.format("%s.%s", clazz.getSimpleName(), field.getName());
				} else {
					key = field.getName();
				}

				boolean accessible = field.isAccessible();
				if (!accessible) {
					field.setAccessible(true);
				}
				
				Object fieldVal = field.get(instance);
				if (fieldVal == null) {
					continue;
				} else if (fieldVal.getClass().isPrimitive() || isWrapperType(fieldVal.getClass())) {
					if (toString(fieldVal, false).isEmpty()) continue;
					output.put(key, toString(fieldVal, false));
				} else if (isMapType(fieldVal.getClass())) {
					//TODO: check if it makes more sense to just stick to json
					// like structure instead of a flatten output.
					Map map = (Map) fieldVal;
					for (Object entry : map.entrySet()) {
						Object mapKey = ((Map.Entry) entry).getKey();
						Object mapVal = ((Map.Entry) entry).getValue();

						String keyStr = getString(mapKey, false);
						String valStr = getString(mapVal, false);
						if ((valStr == null) || (valStr.isEmpty())) {
							continue;
						} else {
							output.put(String.format("%s.%s", key, keyStr), valStr);
						}
					}
				} else if (isCollectionType(fieldVal.getClass())) {
					//TODO check if it makes more sense to just stick to
					// json like structure instead of a flatten output.
					Collection collection = (Collection) fieldVal;
					if (collection.size()==0) continue;
					String outStr = "";
					for (Object o : collection) {
						outStr += getString(o, false) + ",";
					}
					if (outStr.length() > 0) {
						outStr = outStr.substring(0, outStr.length() - 1);
					}
					output.put(key, String.format("%s", outStr));
				} else {
					Map<String, String> nestedFieldValues = getFieldValues(fieldVal, false);
					for (Map.Entry<String, String> entry : nestedFieldValues.entrySet()) {
						output.put(String.format("%s.%s", key, entry.getKey()), entry.getValue());
					}
				}
				if (!accessible) {
					field.setAccessible(false);
				}
			}
		}
		return output;
	}
    
    private static final Set<Class> WRAPPER_TYPES = new HashSet<Class>() {{
		 add(Boolean.class);
		 add(Character.class);
		 add(Byte.class);
		 add(Short.class);
		 add(Integer.class);
		 add(Long.class);
		 add(Float.class);
		 add(Double.class);
		 add(Void.class);
		 add(String.class);
	    }};

	    public static boolean isWrapperType(Class clazz) {
	        return WRAPPER_TYPES.contains(clazz);
	    }

	    public static boolean isCollectionType(Class clazz) {
	        return Collection.class.isAssignableFrom(clazz);
	    }

	    public static boolean isMapType(Class clazz) {
	        return Map.class.isAssignableFrom(clazz);
	    }	

	private static String getString(Object instance,
			boolean wrapWithQuote) throws IllegalAccessException {
		if (instance == null) {
			return null;
		} else if (instance.getClass().isPrimitive() || isWrapperType(instance.getClass())) {
			return toString(instance, wrapWithQuote);
		} else {
			return getString(getFieldValues(instance, false), wrapWithQuote);
		}
	}

	private static String getString(Map<String, String> flattenFields, boolean wrapWithQuote) {
		String outStr = "";
		if (flattenFields != null && !flattenFields.isEmpty()) {
			if (wrapWithQuote) {
				outStr += "\"" + Joiner.on(",").join(flattenFields.entrySet()) + "\",";
			} else {
				outStr += Joiner.on(",").join(flattenFields.entrySet()) + ",";
			}
		}
		
		if (outStr.length() > 0) {
			outStr = outStr.substring(0, outStr.length() - 1);
		}
		return outStr;
	}

	private static String toString(Object instance, boolean wrapWithQuote) {
		if (instance instanceof String)
			if (wrapWithQuote)
				return "\"" + instance + "\"";
			else
				return instance.toString();
		else
			return instance.toString();
	}
	
	private Referenceable getEntityReferenceFromDSL5(final AtlasClient atlasClient, final String typeName, final String dslQuery)
            throws Exception {
		getLogger().info("****************************** Query String: " + dslQuery);
		
        final JSONArray results = atlasClient.searchByDSL(dslQuery);
        getLogger().info("****************************** Query Results Count: " + results.length());
        if (results.length() == 0) {
            return null;
        } else {
            String guid;
            JSONObject row = results.getJSONObject(0);
            if (row.has("$id$")) {
                guid = row.getJSONObject("$id$").getString("id");
            } else {
                guid = row.getJSONObject("_col_0").getString("id");
            }
            getLogger().info("****************************** Resulting JSON Object: " + row.toString());
            getLogger().info("****************************** Inputs to Referenceable: " + guid + " : " + typeName);
            return new Referenceable(guid, typeName, null);
        }
    }
	
	private Referenceable getEntityReferenceFromDSL6(final AtlasClient atlasClient, final String typeName, final String dslQuery)
           throws Exception {
	   getLogger().info("****************************** Query String: " + dslQuery);
	   
       JSONArray results = atlasClient.searchByDSL(dslQuery);
       //JSONArray results = searchDSL(atlasUrl + "/api/atlas/discovery/search/dsl?query=", dslQuery);
	   getLogger().info("****************************** Query Results Count: " + results.length());
       if (results.length() == 0) {
           return null;
       } else {
           String guid;
           JSONObject row = results.getJSONObject(0);
           if (row.has("$id$")) {
               guid = row.getJSONObject("$id$").getString("id");
           } else {
               guid = row.getJSONObject("_col_0").getString("id");
           }
           getLogger().info("****************************** Resulting JSON Object: " + row.toString());
           getLogger().info("****************************** Inputs to Referenceable: " + guid + " : " + typeName);
           return new Referenceable(guid, typeName, null);
       }
   }
	
	public JSONArray searchDSL(String uri, String query){
		query = query.replaceAll(" ", "+");
        getLogger().debug("************************" + query);
        JSONObject json = null;
        JSONArray jsonArray = null;
        try{
        	json = readJsonFromUrl(uri+query);
        	jsonArray = json.getJSONArray("results");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonArray;
    }
	
	private String getAtlasVersion(String urlString, String[] basicAuth){
		System.out.println("************************ Getting Atlas Version from: " + urlString);
		JSONObject json = null;
		String versionValue = null;
        try{
        	json = readJSONFromUrlAuth(urlString, basicAuth);
        	System.out.println("************************ Response from Atlas: " + json);
        	versionValue = json.getString("Version");
        } catch (Exception e) {
            e.printStackTrace();
        }
		return versionValue.substring(0,3);
	}
	
	private JSONObject readJSONFromUrlAuth(String urlString, String[] basicAuth) throws IOException, JSONException {
		String userPassString = basicAuth[0]+":"+basicAuth[1];
		JSONObject json = null;
		try {
            URL url = new URL (urlString);
            String encoding = "YWRtaW46YWRtaW4="; //Base64.encodeBase64String(userPassString.getBytes());

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", "Basic " + encoding);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	json = new JSONObject(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return json;
    }
	
	private JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
	    InputStream is = new URL(url).openStream();
	    try {
	      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
	      String jsonText = readAll(rd);
	      JSONObject json = new JSONObject(jsonText);
	      return json;
	    } finally {
	      is.close();
	    }
	}
	
	private String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	}
	
	private static void createNifiFlowType(){
		  final String typeName = "nifi_flow";
		  final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
				  new AttributeDefinition("nodes", "string", Multiplicity.OPTIONAL, false, null),
				  new AttributeDefinition("flow_id", "string", Multiplicity.OPTIONAL, false, null),
		  };

		  addClassTypeDefinition(typeName, ImmutableSet.of("Process"), attributeDefinitions);
		  System.out.println("Created definition for " + typeName);
	}
	
	private static void createEventType(){
		  final String typeName = "event";
		  final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
				  new AttributeDefinition("event_key", "string", Multiplicity.OPTIONAL, false, null),
		  };

		  addClassTypeDefinition(typeName, ImmutableSet.of("Dataset"), attributeDefinitions);
		  System.out.println("Created definition for " + typeName);
	}
	
	private static void addClassTypeDefinition(String typeName, ImmutableSet<String> superTypes, AttributeDefinition[] attributeDefinitions) {
		HierarchicalTypeDefinition<ClassType> definition =
              new HierarchicalTypeDefinition<>(ClassType.class, typeName, null, superTypes, attributeDefinitions);
		classTypeDefinitions.put(typeName, definition);
	}
	
	public static ImmutableList<EnumTypeDefinition> getEnumTypeDefinitions() {
		return ImmutableList.copyOf(enumTypeDefinitionMap.values());
	}

	public static ImmutableList<StructTypeDefinition> getStructTypeDefinitions() {
		return ImmutableList.copyOf(structTypeDefinitionMap.values());
	}
	
	public static ImmutableList<HierarchicalTypeDefinition<TraitType>> getTraitTypeDefinitions() {
		return ImmutableList.of();
	}
	
	private static String generateAvroSchemaDataModel(){
		TypesDef typesDef;
		String nifiEventLineageDataModelJSON;
		createEventType();
		createNifiFlowType();
		
		typesDef = TypesUtil.getTypesDef(
				getEnumTypeDefinitions(), 	//Enums 
				getStructTypeDefinitions(), //Struct 
				getTraitTypeDefinitions(), 	//Traits 
				ImmutableList.copyOf(classTypeDefinitions.values()));
		
		nifiEventLineageDataModelJSON = TypesSerialization.toJson(typesDef);
		System.out.println("Submitting Types Definition: " + nifiEventLineageDataModelJSON);
		return nifiEventLineageDataModelJSON;
	}
}