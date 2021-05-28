package org.apgupm.processors.couchdb;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.lightcouch.CouchDbException;
import org.lightcouch.DocumentConflictException;
import org.lightcouch.Response;

import java.io.*;
import java.util.List;
import java.util.Set;

@CapabilityDescription("Es capaz de almacenar un documento en una base de datos CouchDB a partir de un FLowfIle en NiFI")
public class CouchDBProcessor  extends AbstractCouchDB {

	public static final Relationship CONFLICT = new Relationship.Builder()
			.name("conflict")
			.description("Muestra los FlowFiles que dan lugar a un conflicto de documentos cuando se almacenan.")
			.build();

	public static final PropertyDescriptor INCLUDE_DOCUMENT = new PropertyDescriptor
			.Builder().name("INCLUDE_DOC")
			.displayName("Incluir documento")
			.description("Incluir o no el documento que se ha guardado (por defecto es falso).")
			.required(false)
			.defaultValue("false")
			.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
			.build();

	public static final Relationship FAILURE = new Relationship.Builder()
			.name("FAILURE")
			.description("Muestra los FlowFiles que no han podido ser almacenados.")
			.build();

	@Override
	protected void initDescriptors(List<PropertyDescriptor> descriptors) {
		descriptors.add(INCLUDE_DOCUMENT);
		super.initDescriptors(descriptors);
	}

	private class SaveDoc implements InputStreamCallback {

		public Response getResponse() {
			return response;
		}
		private Response response = null;
		protected Exception exception = null;
		protected Gson gson = new Gson();

		@Override
		public void process(InputStream in) throws IOException {
			JsonObject inObject = inputStreamToJson(in);
			in.close();

			try {
				response = CouchDBProcessor.this.dbClient.save(inObject);
			} catch (CouchDbException e) {
				exception = e;
				response = null;
			}
		}
		public Exception getException() {
			return exception;
		}

		protected JsonObject inputStreamToJson(InputStream in) {
			Reader lector = new BufferedReader(new InputStreamReader(in));
			return gson.fromJson(lector, JsonObject.class);
		}

	}
	protected boolean createDbClientOrFail(ProcessContext context, ProcessSession session, FlowFile flowFile) {
		try {
			createDbClient(context);
		} catch (ProcessException e) {
			getLogger().error("No se ha podido obtener la conexión a CouchDB", e);

			session.transfer(flowFile, FAILURE);
			return false;
		}
		return true;
	}

	@Override
	protected void initRelationships(Set relationships) {
		relationships.add(CONFLICT);
		super.initRelationships(relationships);
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

		FlowFile flowFile = session.get();
		if (flowFile == null)
			return;

		if (!createDbClientOrFail(context, session, flowFile))
			return;

		final SaveDoc save = new SaveDoc();
		session.read(flowFile, save);

		if (save.getException() != null) {
			if (save.getException().getClass() == DocumentConflictException.class) {
				session.transfer(flowFile, CONFLICT);
				return;
			} else
				throw new ProcessException("Error guardando el documento", save.getException());
		}

		if (save.getResponse() != null && save.getResponse().getError() != null) {
			getLogger().warn(String.format("Error guardando el documento: %s; razon: %s", save.getResponse().getError(), save.getResponse().getReason()));
			session.rollback();

			session.transfer(flowFile, FAILURE);
			return;
		}
		if (SUCCESS.isAutoTerminated()) {
			session.remove(flowFile);
			return;
		}

		if (context.getProperty(INCLUDE_DOCUMENT).asBoolean()) {
			InputStream objectStream = dbClient.find(save.getResponse().getId(), save.getResponse().getRev());
			session.importFrom(objectStream, flowFile);
			try {
				objectStream.close();
			} catch (IOException e) {
				throw new ProcessException(e);
			}
		} else {
			session.write(flowFile, new OutputStreamCallback() {

				@Override
				public void process(OutputStream out) throws IOException {
					Gson gson = new Gson();
					String json = gson.toJson(save.getResponse());
					out.write(json.getBytes());
				}
			});
		}
		flowFile = setCoreAttributes(session, flowFile, save.getResponse().getId() + "@" + save.getResponse().getRev());
		session.transfer(flowFile, SUCCESS);
	}

}

