package com.infinitbyte.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.get.GetField;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.RequestBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 * Created by IntelliJ IDEA.
 * User: Medcl'
 * Date: 9/15/11
 * Time: 3:09 PM
 */
public class PartialUpdateRestAction extends BaseRestHandler {

    @Inject
    public PartialUpdateRestAction(Settings settings, Client client, RestController restController) {
        super(settings, client);

        restController.registerHandler(RestRequest.Method.PUT,"/{index}/{type}/{id}/_reindex",this);
        restController.registerHandler(RestRequest.Method.POST,"/{index}/{type}/{id}/_reindex",this);
        restController.registerHandler(RestRequest.Method.POST,"/{index}/{type}/{id}/_update",this);
        restController.registerHandler(RestRequest.Method.PUT,"/{index}/{type}/{id}/_update",this);
    }

    public void handleRequest(final RestRequest request, final RestChannel channel) {
        if(logger.isDebugEnabled()){
            logger.debug("partial reindex entering...");
        }

        if(logger.isDebugEnabled()){
            logger.debug("doc pending to be update:{}/{}/{}",request.param("index"), request.param("type"), request.param("id"));
        }

        final GetRequest getRequest = new GetRequest(request.param("index"), request.param("type"), request.param("id"));
        getRequest.routing(request.param("routing"));
        getRequest.preference(request.param("preference"));
//        getRequest.realtime(request.paramAsBoolean("realtime", null));

        // no need to have a threaded listener since we just send back a response
        getRequest.listenerThreaded(false);
        // if we have a local operation, execute it on a thread since we don't spawn
        getRequest.operationThreaded(true);

         //get original document
        client.get(getRequest,new ActionListener<GetResponse>() {
            public void onResponse(GetResponse getResponse) {

                if(logger.isDebugEnabled())
                {
                    logger.debug("entering get response...");
                }

                 try {
                    XContentBuilder builder = restContentBuilder(request);
                    getResponse.toXContent(builder, request);
                    if (!getResponse.exists()) {
                        channel.sendResponse(new XContentRestResponse(request, NOT_FOUND, builder));
                    } else {
                        logger.info(getResponse.sourceAsString());
                        if(!getResponse.isSourceEmpty()){
                            Map<String,Object> source=getResponse.getSource();
                            if(logger.isDebugEnabled())
                            {
                                logger.debug("there are {} fields in the source,iterating",source.size());
                            }

                            //prepare document
                            for (Iterator<String> iterator = source.keySet().iterator(); iterator.hasNext(); ) {
                                String next =  iterator.next();

                                if(logger.isDebugEnabled()){
                                logger.debug("key:{},value:{}",next,source.get(next));
                                }
                            }

                            //source.put("_modified", new DateTime());

                            //indexing
                            IndexRequest indexRequest = new IndexRequest(request.param("index"), request.param("type"), request.param("id"));
                            indexRequest.routing(request.param("routing"));
                            indexRequest.parent(request.param("parent"));
                            indexRequest.source(source);
                            indexRequest.timeout(request.paramAsTime("timeout", IndexRequest.DEFAULT_TIMEOUT));
                            indexRequest.refresh(request.paramAsBoolean("refresh", indexRequest.refresh()));
                            indexRequest.version(RestActions.parseVersion(request));
                            indexRequest.versionType(VersionType.fromString(request.param("version_type"), indexRequest.versionType()));
                            indexRequest.percolate(request.param("percolate", null));
                            indexRequest.opType(IndexRequest.OpType.INDEX);
                            String replicationType = request.param("replication");
                            if (replicationType != null) {
                                indexRequest.replicationType(ReplicationType.fromString(replicationType));
                            }
                            String consistencyLevel = request.param("consistency");
                            if (consistencyLevel != null) {
                                indexRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
                            }
                            // we just send a response, no need to fork
                            indexRequest.listenerThreaded(false);
                            // we don't spawn, then fork if local
                            indexRequest.operationThreaded(true);

                            if(logger.isDebugEnabled()){
                                logger.debug("ready to indexing");
                            }

                             client.index(indexRequest, new ActionListener<IndexResponse>() {
                                 public void onResponse(IndexResponse response) {

                                    if(logger.isDebugEnabled()){
                                        logger.debug("entering index response...");
                                    }
                                    try {
                                        XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                                        builder.startObject()
                                                .field(Fields.OK, true)
                                                .field(Fields._INDEX, response.index())
                                                .field(Fields._TYPE, response.type())
                                                .field(Fields._ID, response.id())
                                                .field(Fields._VERSION, response.version());
                                        if (response.matches() != null) {
                                            builder.startArray(Fields.MATCHES);
                                            for (String match : response.matches()) {
                                                builder.value(match);
                                            }
                                            builder.endArray();
                                        }
                                        builder.endObject();
                                        RestStatus status = OK;
                                        if (response.version() == 1) {
                                            status = CREATED;
                                        }
                                        channel.sendResponse(new XContentRestResponse(request, status, builder));
                                    } catch (Exception e) {
                                        onFailure(e);
                                    }
                                }

                                 public void onFailure(Throwable e) {
                                    try {
                                        channel.sendResponse(new XContentThrowableRestResponse(request, e));
                                    } catch (IOException e1) {
                                        logger.error("Failed to send failure response", e1);
                                    }
                                }
                            });


                        }else {
                            builder = RestXContentBuilder.restContentBuilder(request);
                                        builder.startObject().field("reason","source is empty").endObject();
                             channel.sendResponse(new XContentRestResponse(request,RestStatus.BAD_REQUEST, builder));
                        }

                    }
                } catch (Exception e) {
                    onFailure(e);
                }

            }

            public void onFailure(Throwable e) {
                     try{
                         channel.sendResponse(new XContentThrowableRestResponse(request,e));
                     } catch (IOException e1) {
                         e1.printStackTrace();
                         logger.error("failed to send failure response",e1);
                     }
            }
        });

    }

        static final class Fields {
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
    }
}
