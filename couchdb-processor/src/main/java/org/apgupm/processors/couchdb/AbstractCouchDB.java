package org.apgupm.processors.couchdb;


import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbException;
import org.lightcouch.CouchDbProperties;
import java.util.*;

public abstract class AbstractCouchDB extends AbstractProcessor {

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    protected CouchDbClient dbClient = null;

    public static final PropertyDescriptor SERVER = new PropertyDescriptor
            .Builder().name("SERVER")
            .displayName("Dirección del servidor")
            .description("Dirección del CouchDB al que conectarse.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("PORT")
            .displayName("Puerto del servidor")
            .description("Puerto de CouchDB al que conectarse (por defecto 5984).")
            .required(false)
            .defaultValue("5984")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder().name("USERNAME")
            .displayName("Usuario")
            .description("Nombre del usuario para iniciar sesión en el servidor CouchDB.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("PASSWORD")
            .displayName("Contraseña")
            .description("Contraseña del usuario para iniciar sesión en el servidor CouchDB.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor
            .Builder().name("DATABASE")
            .displayName("Base de datos")
            .description("Database to connect to on the CouchDB server.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AUTOCREATE = new PropertyDescriptor
            .Builder().name("AUTOCREATE")
            .displayName("Autocreación")
            .description("Crear o no la base de datos si no existe (por defecto es false).")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Da salida a la respuesta JSON del servidor CouchDB.")
            .build();


    protected void initDescriptors(List<PropertyDescriptor> descriptors) {
    	this.descriptors = Collections.unmodifiableList(descriptors);
    }
    
    protected void initRelationships(Set<Relationship> relationships) {
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SERVER);
        descriptors.add(PORT);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(DATABASE);
        descriptors.add(AUTOCREATE);
        initDescriptors(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        initRelationships(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    protected void createDbClient(ProcessContext context) throws ProcessException  {
		try {
			CouchDbProperties properties = new CouchDbProperties()
					  .setDbName(context.getProperty(DATABASE).getValue())
					  .setCreateDbIfNotExist(context.getProperty(AUTOCREATE).asBoolean())
					  .setProtocol("http")
					  .setHost(context.getProperty(SERVER).getValue())
					  .setPort(context.getProperty(PORT).asInteger())
					  .setUsername(context.getProperty(USERNAME).getValue())
					  .setPassword(context.getProperty(PASSWORD).getValue())
					  .setMaxConnections(100)
					  .setConnectionTimeout(0);
	
			this.dbClient = new CouchDbClient(properties);
		} catch (CouchDbException e) {
			throw new ProcessException(e);
		}
	}

    protected final FlowFile setCoreAttributes(ProcessSession session, FlowFile flowFile, String filename) {
		flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), filename);
		flowFile = session.putAttribute(flowFile, CoreAttributes.PATH.key(), this.dbClient.getDBUri().toString());
		flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
		return flowFile;
    }

	
}
