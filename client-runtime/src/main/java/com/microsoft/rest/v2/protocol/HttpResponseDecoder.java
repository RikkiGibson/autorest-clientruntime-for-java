/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.rest.v2.protocol;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.reflect.TypeToken;
import com.microsoft.rest.v2.Base64Url;
import com.microsoft.rest.v2.DateTimeRfc1123;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.SwaggerMethodParser;
import com.microsoft.rest.v2.UnixTime;
import com.microsoft.rest.v2.http.HttpHeaders;
import com.microsoft.rest.v2.http.HttpResponse;
import com.microsoft.rest.v2.util.FlowableUtil;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import org.joda.time.DateTime;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deserializes an {@link HttpResponse}.
 */
public final class HttpResponseDecoder {
    private final SwaggerMethodParser methodParser;
    private final SerializerAdapter<?> serializer;

    /**
     * Creates an HttpResponseDecoder.
     * @param methodParser metadata about the Swagger method used for decoding.
     * @param serializer the serializer
     */
    public HttpResponseDecoder(SwaggerMethodParser methodParser, SerializerAdapter<?> serializer) {
        this.methodParser = methodParser;
        this.serializer = serializer;
    }

    /**
     * Asynchronously decodes an {@link HttpResponse}, deserializing into a response or error value.
     * @param response the {@link HttpResponse}
     * @return A {@link Single} containing either the decoded HttpResponse or an error
     */
    public Single<HttpResponse> decode(final HttpResponse response) {
        final Object deserializedHeaders;
        try {
            deserializedHeaders = deserializeHeaders(response.headers());
        } catch (IOException e) {
            return Single.error(e);
        }

        final Type returnValueWireType = methodParser.returnValueWireType();

        final TypeToken entityTypeToken = getEntityType();

        boolean isSerializableBody = !FlowableUtil.isFlowablePooledBuffer(entityTypeToken)
            && !entityTypeToken.isSubtypeOf(Completable.class)
            && !entityTypeToken.isSubtypeOf(byte[].class)
            && !entityTypeToken.isSubtypeOf(boolean.class) && !entityTypeToken.isSubtypeOf(Boolean.class)
            && !entityTypeToken.isSubtypeOf(Void.TYPE) && !entityTypeToken.isSubtypeOf(Void.class);

        Single<HttpResponse> result = ensureExpectedStatus(response, methodParser);

        if (isSerializableBody) {
            result = result.toCompletable().andThen(response.bodyAsStringAsync().map(new Function<String, HttpResponse>() {
                @Override
                public HttpResponse apply(String bodyString) throws Exception {
                    Object body = deserialize(bodyString, getEntityType().getType(), returnValueWireType, SerializerEncoding.fromHeaders(response.headers()));
                    return response
                            .withDeserializedHeaders(deserializedHeaders)
                            .withDeserializedBody(body);
                }
            }));
        } else {
            result = result.toCompletable().andThen(Single.just(response.withDeserializedHeaders(deserializedHeaders)));
        }

        return result;
    }

    private Object deserialize(String value, Type resultType, Type wireType, SerializerEncoding encoding) throws IOException {
        Object result;

        if (wireType == null) {
            result = serializer.deserialize(value, resultType, encoding);
        }
        else {
            final Type wireResponseType = constructWireResponseType(resultType, wireType);
            final Object wireResponse = serializer.deserialize(value, wireResponseType, encoding);
            result = convertToResultType(wireResponse, resultType, wireType);
        }

        return result;
    }

    private static Type[] getTypeArguments(Type type) {
        return ((ParameterizedType) type).getActualTypeArguments();
    }

    private static Type getTypeArgument(Type type) {
        return getTypeArguments(type)[0];
    }

    private Type constructWireResponseType(Type resultType, Type wireType) {
        Type wireResponseType = resultType;

        if (resultType == byte[].class) {
            if (wireType == Base64Url.class) {
                wireResponseType = Base64Url.class;
            }
        }
        else if (resultType == DateTime.class) {
            if (wireType == DateTimeRfc1123.class) {
                wireResponseType = DateTimeRfc1123.class;
            }
            else if (wireType == UnixTime.class) {
                wireResponseType = UnixTime.class;
            }
        }
        else {
            final TypeToken resultTypeToken = TypeToken.of(resultType);
            if (resultTypeToken.isSubtypeOf(List.class)) {
                final Type resultElementType = getTypeArgument(resultType);
                final Type wireResponseElementType = constructWireResponseType(resultElementType, wireType);

                final TypeFactory typeFactory = serializer.getTypeFactory();
                wireResponseType = typeFactory.create((ParameterizedType) resultType, wireResponseElementType);
            }
            else if (resultTypeToken.isSubtypeOf(Map.class) || resultTypeToken.isSubtypeOf(RestResponse.class)) {
                Type[] typeArguments = getTypeArguments(resultType);
                final Type resultValueType = typeArguments[1];
                final Type wireResponseValueType = constructWireResponseType(resultValueType, wireType);

                final TypeFactory typeFactory = serializer.getTypeFactory();
                wireResponseType = typeFactory.create((ParameterizedType) resultType, new Type[] {typeArguments[0], wireResponseValueType});
            }
        }
        return wireResponseType;
    }

    private Object convertToResultType(Object wireResponse, Type resultType, Type wireType) {
        Object result = wireResponse;

        if (wireResponse != null) {
            if (resultType == byte[].class) {
                if (wireType == Base64Url.class) {
                    result = ((Base64Url) wireResponse).decodedBytes();
                }
            } else if (resultType == DateTime.class) {
                if (wireType == DateTimeRfc1123.class) {
                    result = ((DateTimeRfc1123) wireResponse).dateTime();
                } else if (wireType == UnixTime.class) {
                    result = ((UnixTime) wireResponse).dateTime();
                }
            } else {
                final TypeToken resultTypeToken = TypeToken.of(resultType);
                if (resultTypeToken.isSubtypeOf(List.class)) {
                    final Type resultElementType = getTypeArgument(resultType);

                    final List<Object> wireResponseList = (List<Object>) wireResponse;

                    final int wireResponseListSize = wireResponseList.size();
                    for (int i = 0; i < wireResponseListSize; ++i) {
                        final Object wireResponseElement = wireResponseList.get(i);
                        final Object resultElement = convertToResultType(wireResponseElement, resultElementType, wireType);
                        if (wireResponseElement != resultElement) {
                            wireResponseList.set(i, resultElement);
                        }
                    }

                    result = wireResponseList;
                }
                else if (resultTypeToken.isSubtypeOf(Map.class)) {
                    final Type resultValueType = getTypeArguments(resultType)[1];

                    final Map<String, Object> wireResponseMap = (Map<String, Object>) wireResponse;

                    final Set<String> wireResponseKeys = wireResponseMap.keySet();
                    for (String wireResponseKey : wireResponseKeys) {
                        final Object wireResponseValue = wireResponseMap.get(wireResponseKey);
                        final Object resultValue = convertToResultType(wireResponseValue, resultValueType, wireType);
                        if (wireResponseValue != resultValue) {
                            wireResponseMap.put(wireResponseKey, resultValue);
                        }
                    }
                } else if (resultTypeToken.isSubtypeOf(RestResponse.class)) {
                    RestResponse<?, ?> restResponse = (RestResponse<?, ?>) wireResponse;
                    Object wireResponseBody = restResponse.body();

                    Object resultBody = convertToResultType(wireResponseBody, getTypeArguments(resultType)[1], wireType);
                    if (wireResponseBody != resultBody) {
                        result = new RestResponse<>(restResponse.statusCode(), restResponse.headers(), restResponse.rawHeaders(), resultBody);
                    } else {
                        result = restResponse;
                    }
                }
            }
        }

        return result;
    }

    private TypeToken getEntityType() {
        TypeToken token = TypeToken.of(methodParser.returnType());

        if (token.isSubtypeOf(Single.class) || token.isSubtypeOf(Maybe.class)) {
            token = TypeToken.of(getTypeArgument(token.getType()));
        }

        if (token.isSubtypeOf(RestResponse.class)) {
            token = TypeToken.of(getTypeArguments(token.getType())[1]);
        }

        return token;
    }

    private Type getHeadersType() {
        TypeToken token = TypeToken.of(methodParser.returnType());
        Type headersType = null;

        if (token.isSubtypeOf(Single.class)) {
            token = TypeToken.of(getTypeArgument(token.getType()));
        }

        if (token.isSubtypeOf(RestResponse.class)) {
            headersType = getTypeArguments(token.getType())[0];
        }

        return headersType;
    }

    private Object deserializeHeaders(HttpHeaders headers) throws IOException {
            final Type deserializedHeadersType = getHeadersType();
            if (deserializedHeadersType == null) {
                return null;
            } else {
                final String headersJsonString = serializer.serialize(headers, SerializerEncoding.JSON);
                Object deserializedHeaders = serializer.deserialize(headersJsonString, deserializedHeadersType, SerializerEncoding.JSON);
                return deserializedHeaders;
            }
    }

    private Exception instantiateUnexpectedException(SwaggerMethodParser methodParser, HttpResponse response, String responseContent) {
        final int responseStatusCode = response.statusCode();
        final Class<? extends RestException> exceptionType = methodParser.exceptionType();
        final Class<?> exceptionBodyType = methodParser.exceptionBodyType();

        String contentType = response.headerValue("Content-Type");
        String bodyRepresentation;
        if ("application/octet-stream".equalsIgnoreCase(contentType)) {
            bodyRepresentation = "(" + response.headerValue("Content-Length") + "-byte body)";
        } else {
            bodyRepresentation = responseContent.isEmpty() ? "(empty body)" : "\"" + responseContent + "\"";
        }

        Exception result;
        try {
            final Constructor<? extends RestException> exceptionConstructor = exceptionType.getConstructor(String.class, HttpResponse.class, exceptionBodyType);

            boolean isSerializableContentType = contentType == null || contentType.isEmpty()
                    || contentType.startsWith("application/json")
                    || contentType.startsWith("text/json")
                    || contentType.startsWith("application/xml")
                    || contentType.startsWith("text/xml");

            final Object exceptionBody = responseContent.isEmpty() || !isSerializableContentType
                    ? null
                    : serializer.deserialize(responseContent, exceptionBodyType, SerializerEncoding.fromHeaders(response.headers()));

            result = exceptionConstructor.newInstance("Status code " + responseStatusCode + ", " + bodyRepresentation, response, exceptionBody);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException | JsonParseException e) {
            String message = "Status code " + responseStatusCode + ", but an instance of "
                    + exceptionType.getCanonicalName() + " cannot be created."
                    + " Response body: " + bodyRepresentation;

            result = new IOException(message, e);
        } catch (IOException e) {
            result = e;
        }

        return result;
    }

    Single<HttpResponse> ensureExpectedStatus(final HttpResponse response, final SwaggerMethodParser methodParser) {
        return ensureExpectedStatus(response, methodParser, null);
    }

    /**
     * Ensure that the provided HttpResponse has a status code that is defined in the provided
     * SwaggerMethodParser or is in the int[] of additional allowed status codes. If the
     * HttpResponse's status code is not allowed, then an exception will be thrown.
     * @param response The HttpResponse to check.
     * @param methodParser The method parser that contains information about the service interface
     *                     method that initiated the HTTP request.
     * @param additionalAllowedStatusCodes Additional allowed status codes that are permitted based
     *                                     on the context of the HTTP request.
     * @return An async-version of the provided HttpResponse.
     */
    public Single<HttpResponse> ensureExpectedStatus(final HttpResponse response, final SwaggerMethodParser methodParser, int[] additionalAllowedStatusCodes) {
        final int responseStatusCode = response.statusCode();
        final Single<HttpResponse> asyncResult;
        if (!methodParser.isExpectedResponseStatusCode(responseStatusCode, additionalAllowedStatusCodes)) {
            asyncResult = response.bodyAsStringAsync().flatMap(new Function<String, Single<HttpResponse>>() {
                @Override
                public Single<HttpResponse> apply(String responseBody) throws Exception {
                    return Single.error(instantiateUnexpectedException(methodParser, response, responseBody));
                }
            });
        } else {
            asyncResult = Single.just(response);
        }

        return asyncResult;
    }
}