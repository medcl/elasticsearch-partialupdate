package org.elasticsearch.plugin.infinitbyte;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.lzf.LZFChunk;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.CachedStreamInput;
import org.elasticsearch.common.io.stream.HandlesStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.*;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

/**
 * Created by IntelliJ IDEA. User: Medcl' Date: 9/15/11 Time: 3:09 PM
 */
public class PartialUpdateRestAction extends BaseRestHandler {

    @Inject
    public PartialUpdateRestAction(Settings settings, Client client,
                                   RestController restController) {
        super(settings, client);
        restController.registerHandler(RestRequest.Method.POST,"/{index}/{type}/{id}/_partial_update", this);
        restController.registerHandler(RestRequest.Method.PUT,"/{index}/{type}/{id}/_partial_update", this);
        restController.registerHandler(RestRequest.Method.POST,"/{index}/{type}/{id}/_partial_update/{array_merge}", this);
        restController.registerHandler(RestRequest.Method.PUT,"/{index}/{type}/{id}/_partial_update/{array_merge}", this);
    }

    public void handleRequest(final RestRequest request,
                              final RestChannel channel) {
        if (logger.isDebugEnabled()) {
            logger.debug("partial update entering...");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("doc pending to be update:{}/{}/{}",
                    request.param("index"), request.param("type"),
                    request.param("id"));
        }

       final String mergeMethod=request.param("array_merge","replace");

       final String sourceJson=sourceAsString(request.content());

        final JSONObject pendingChangeJsonObject=JSON.parseObject(sourceJson);


        // if pending changes is empty,just return
        if (pendingChangeJsonObject.size() <= 0) {
            XContentBuilder builder = null;
            try {

                builder = RestXContentBuilder.restContentBuilder(request);
                builder.startObject()
                        .field("reason", "pending changes is empty")
                        .endObject();

                channel.sendResponse(new XContentRestResponse(request,
                        RestStatus.BAD_REQUEST, builder));
                return;
            } catch (IOException e) {
                logger.error("update",e);
            }
        }


        final GetRequest getRequest = new GetRequest(request.param("index"),
                request.param("type"), request.param("id"));
        getRequest.routing(request.param("routing"));
        getRequest.preference(request.param("preference"));
        // getRequest.realtime(request.paramAsBoolean("realtime", null));

        // no need to have a threaded listener since we just send back a
        // response
        getRequest.listenerThreaded(false);
        // if we have a local operation, execute it on a thread since we don't
        // spawn
        getRequest.operationThreaded(true);

        // get original document
        client.get(getRequest, new ActionListener<GetResponse>() {
            public void onResponse(GetResponse getResponse) {

                if (logger.isDebugEnabled()) {
                    logger.debug("entering get response...");
                }

                try {
                    XContentBuilder builder = restContentBuilder(request);
                    getResponse.toXContent(builder, request);
                    if (!getResponse.isExists()) {
                        channel.sendResponse(new XContentRestResponse(request,
                                NOT_FOUND, builder));
                    } else {

                        if (logger.isDebugEnabled()) {
                            logger.debug(getResponse.getSourceAsString());
                        }

                        if (!getResponse.isSourceEmpty()) {
                            String source= getResponse.getSourceAsString();
                            logger.debug("origin:"+source);
                            logger.debug("pending:"+sourceJson);
                            JSONObject jsonObject = JSON.parseObject(source);

                            // prepare document
                            Iterator<String> iterator;
                            for (iterator=pendingChangeJsonObject.keySet().iterator(); iterator.hasNext();) {
                                String next = iterator.next();
                                logger.debug("update:"+next);
                                if(jsonObject.containsKey(next)){
                                    Object jo = jsonObject.get(next);
                                    if(jo instanceof JSONArray){
                                        JSONArray jArray = ((JSONArray) jo);
                                        Object v=pendingChangeJsonObject.get(next);

                                        boolean pendingDataIsArray=false;
                                        JSONArray pArray=null;

                                        String trim = v.toString().trim();
                                        if(trim.startsWith("[")&& trim.endsWith("]")){
                                            try{
                                                pArray =JSON.parseArray(trim);
                                                if(pArray.size()>0){
                                                    pendingDataIsArray=true;
                                                }
                                            }catch (Exception e){
                                               logger.error("update",e);
                                            }

                                        }

                                        if(mergeMethod.equals("append")){
                                               if(pendingDataIsArray){
                                                   for (int i = 0; i < pArray.size(); i++) {
                                                       if(!jArray.contains(pArray.get(i))){
                                                           jArray.add(pArray.get(i));
                                                       }
                                                   }
                                               } else{
                                                   jArray.add(v);
                                               }
                                                jsonObject.put(next,jArray);
                                        }else  if(mergeMethod.equals("remove")){
                                                if(pendingDataIsArray){
                                                    for (Object aPArray : pArray) {
                                                        if (jArray.contains(aPArray)) {
                                                            jArray.remove(aPArray);
                                                        }
                                                    }
                                                }else{
                                                if(jArray.contains(v)){
                                                    jArray.remove(v);
                                                }
                                            }
                                            jsonObject.put(next,jArray);
                                        }else{
                                            jsonObject.put(next,pendingChangeJsonObject.get(next));
                                        }
                                    }else{
                                     jsonObject.put(next,pendingChangeJsonObject.get(next));
                                    }
                                }else{
                                  jsonObject.put(next,pendingChangeJsonObject.get(next));
                                }
                            }

                            long epoch = System.currentTimeMillis()/1000;

                            jsonObject.put("_last_partial_updated", epoch);


                            logger.debug("update json:"+jsonObject.toJSONString());

                            // indexing
                            IndexRequest indexRequest = new IndexRequest(
                                    request.param("index"), request
                                    .param("type"), request.param("id"));
                            indexRequest.routing(request.param("routing"));
                            indexRequest.parent(request.param("parent"));
                            indexRequest.source(jsonObject.toJSONString());
                            indexRequest.timeout(request.paramAsTime("timeout",
                                    IndexRequest.DEFAULT_TIMEOUT));
                            indexRequest.refresh(request.paramAsBoolean(
                                    "refresh", indexRequest.refresh()));
                            indexRequest.version(RestActions
                                    .parseVersion(request));
                            indexRequest.versionType(VersionType.fromString(
                                    request.param("version_type"),
                                    indexRequest.versionType()));

                            indexRequest.opType(IndexRequest.OpType.INDEX);
                            String replicationType = request
                                    .param("replication");
                            if (replicationType != null) {
                                indexRequest.replicationType(ReplicationType
                                        .fromString(replicationType));
                            }
                            String consistencyLevel = request
                                    .param("consistency");
                            if (consistencyLevel != null) {
                                indexRequest
                                        .consistencyLevel(WriteConsistencyLevel
                                                .fromString(consistencyLevel));
                            }
                            // we just send a response, no need to fork
                            indexRequest.listenerThreaded(false);
                            // we don't spawn, then fork if local
                            indexRequest.operationThreaded(true);

                            if (logger.isDebugEnabled()) {
                                logger.debug("ready to indexing");
                            }

                            client.index(indexRequest,
                                    new ActionListener<IndexResponse>() {
                                        public void onResponse(
                                                IndexResponse response) {

                                            if (logger.isDebugEnabled()) {
                                                logger.debug("entering index response...");
                                            }
                                            try {
                                                XContentBuilder builder = RestXContentBuilder
                                                        .restContentBuilder(request);
                                                builder.startObject()
                                                        .field(Fields.OK, true)
                                                        .field(Fields._INDEX,
                                                                response.getIndex())
                                                        .field(Fields._TYPE,
                                                                response.getType())
                                                        .field(Fields._ID,
                                                                response.getId())
                                                        .field(Fields._VERSION,
                                                                response.getVersion());

                                                builder.endObject();
                                                RestStatus status = OK;
                                                if (response.getVersion() == 1) {
                                                    status = CREATED;
                                                }
                                                channel.sendResponse(new XContentRestResponse(
                                                        request, status,
                                                        builder));
                                            } catch (Exception e) {
                                                onFailure(e);
                                            }
                                            if (logger.isDebugEnabled()) {
                                                logger.debug("exit index response");
                                            }
                                        }

                                        public void onFailure(Throwable e) {
                                            try {
                                                channel.sendResponse(new XContentThrowableRestResponse(
                                                        request, e));
                                            } catch (IOException e1) {
                                                logger.error(
                                                        "Failed to send failure response",
                                                        e1);
                                            }
                                        }

                                    });

                        } else {
                            builder = RestXContentBuilder
                                    .restContentBuilder(request);
                            builder.startObject()
                                    .field("reason", "source is empty")
                                    .endObject();
                            channel.sendResponse(new XContentRestResponse(
                                    request, RestStatus.BAD_REQUEST, builder));
                        }

                    }
                } catch (Exception e) {
                    onFailure(e);
                }

            }

            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(
                            request, e));
                } catch (IOException e1) {
                    e1.printStackTrace();
                    logger.error("failed to send failure response", e1);
                }
            }
        });
        if (logger.isDebugEnabled()) {
            logger.debug("exit partial update");
        }
    }

    public String sourceAsString(BytesReference source ) {

        if(source!=null){
            try {
                return XContentHelper.convertToJson(source, false);
            } catch (IOException e) {
                throw new ElasticSearchParseException("failed to convert source to a json string");
            }
        }
        return  null;
    }

    public static Map<String, Object> sourceAsMap(byte[] bytes, int offset,
                                                  int length) {
        XContentParser parser = null;
        try {
            if (isCompressed(bytes, offset, length)) {
                BytesStreamInput siBytes = new BytesStreamInput(bytes, offset,
                        length, true);
                HandlesStreamInput siLzf = CachedStreamInput
                        .cachedHandles(siBytes);
                XContentType contentType = XContentFactory.xContentType(siLzf);
                siLzf.reset();
                parser = XContentFactory.xContent(contentType).createParser(
                        siLzf);
                return parser.map();
            } else {
                parser = XContentFactory.xContent(bytes, offset, length)
                        .createParser(bytes, offset, length);
                return parser.map();
            }
        } catch (Exception e) {
            throw new ElasticSearchParseException(
                    "Failed to parse source to map", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public static Map<String, Object> sourceAsMap(byte[] bytes, int offset,
                                                  int length, boolean unsafe) {
        XContentParser parser = null;
        try {
            if (isCompressed(bytes, offset, length)) {
                BytesStreamInput siBytes = new BytesStreamInput(bytes, offset,
                        length, unsafe);
                HandlesStreamInput siLzf = CachedStreamInput
                        .cachedHandles(siBytes);
                XContentType contentType = XContentFactory.xContentType(siLzf);
                siLzf.reset();
                parser = XContentFactory.xContent(contentType).createParser(
                        siLzf);
                return parser.map();
            } else {
                parser = XContentFactory.xContent(bytes, offset, length)
                        .createParser(bytes, offset, length);
                return parser.map();
            }
        } catch (Exception e) {
            throw new ElasticSearchParseException(
                    "Failed to parse source to map", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    public static boolean isCompressed(final byte[] buffer, int offset,
                                       int length) {
        return length >= 2 && buffer[offset] == LZFChunk.BYTE_Z
                && buffer[offset + 1] == LZFChunk.BYTE_V;
    }

    static final class Fields {
        static final XContentBuilderString OK = new XContentBuilderString("ok");
        static final XContentBuilderString _INDEX = new XContentBuilderString(
                "_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString(
                "_type");
        static final XContentBuilderString _ID = new XContentBuilderString(
                "_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString(
                "_version");
        static final XContentBuilderString MATCHES = new XContentBuilderString(
                "matches");
    }
}