package com.hortonworks.nifi;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StandardLineageResult;
import org.apache.nifi.provenance.StandardQueryResult;
import org.apache.nifi.provenance.lineage.LineageNodeType;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.SearchTerm;
import org.apache.nifi.provenance.search.SearchTerms;
import org.apache.nifi.provenance.search.SearchableField;

@SideEffectFree
@Tags({"Atlas", "Governance"})
@CapabilityDescription("Gather lineage information to register with Atlas")
public class AtlasLineageProcessor extends AbstractProcessor {
	//private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;

	public static final String MATCH_ATTR = "match";

	/*public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
	        .name("Json Path")
	        .required(true)
	        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
	        .build();
	*/
	public static final Relationship SUCCESS = new Relationship.Builder()
	        .name("SUCCESS")
	        .description("Succes relationship")
	        .build();
	
	public void init(final ProcessorInitializationContext context){
	    /*List<PropertyDescriptor> properties = new ArrayList<>();
	    properties.add(JSON_PATH);
	    this.properties = Collections.unmodifiableList(properties);
		*/
	    Set<Relationship> relationships = new HashSet<Relationship>();
	    relationships.add(SUCCESS);
	    this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships(){
	    return relationships;
	}

	/*
	public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
	    return properties;
	}
	*/
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		final ProcessorLog log = this.getLogger();
		Set<String> lineageIds = new HashSet<String>();
		FlowFile flowFile = session.get();
		ProvenanceReporter provRep = session.getProvenanceReporter();
		SearchableField lineageEventType = SearchableFields.getSearchableField("EventType");
		SearchableField termFlowFileUUID = SearchableFields.getSearchableField("FlowFileUUID");
		Query lineageQuery = new Query("123456789");
		lineageQuery.addSearchTerm(SearchTerms.newSearchTerm(lineageEventType, "RECEIVE"));
		lineageQuery.addSearchTerm(SearchTerms.newSearchTerm(lineageEventType, "CLONE"));
		lineageQuery.addSearchTerm(SearchTerms.newSearchTerm(termFlowFileUUID, flowFile.getAttribute("uuid")));
		StandardQueryResult lineageResults = new StandardQueryResult(lineageQuery, 1);
		List<ProvenanceEventRecord> lineageEventRecords = lineageResults.getMatchingEvents();
		
		if(lineageEventRecords.size() > 0){
			log.info(lineageEventRecords.get(0).toString());
			provRep.send(flowFile, lineageEventRecords.get(0).toString());
		}else{
			log.info("No Results");
			provRep.send(flowFile, "No Results");
		}
		session.transfer(flowFile, SUCCESS);
	}	   
}