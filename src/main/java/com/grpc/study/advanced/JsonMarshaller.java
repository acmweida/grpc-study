package com.grpc.study.advanced;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat.Parser;
import com.google.protobuf.util.JsonFormat.Printer;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.io.*;
import java.nio.charset.Charset;

public class JsonMarshaller {

    private JsonMarshaller() {
    }

    public static <T extends Message>MethodDescriptor.Marshaller<T> jsonMarshaller(T defaultInstance) {
        final Parser parser = JsonFormat.parser();
        final Printer printer = JsonFormat.printer();
        return jsonMarshaller(defaultInstance,parser,printer);
    }

    public static <T extends Message> MethodDescriptor.Marshaller<T> jsonMarshaller(
            final T defaultInstance, final Parser parser, final Printer printer) {

        final Charset charset = Charset.forName("UTF-8");

        return new MethodDescriptor.Marshaller<T>() {
            @Override
            public InputStream stream(T value) {
                try {
                    return new ByteArrayInputStream(printer.print(value).getBytes(charset));
                } catch (InvalidProtocolBufferException e) {
                    throw Status.INTERNAL
                            .withCause(e)
                            .withDescription("Unable to print json proto")
                            .asRuntimeException();
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public T parse(InputStream stream) {
                Message.Builder builder = defaultInstance.newBuilderForType();
                Reader reader = new InputStreamReader(stream, charset);
                T proto;
                try {
                    parser.merge(reader, builder);
                    proto = (T) builder.build();
                    reader.close();
                } catch (InvalidProtocolBufferException e) {
                    throw Status.INTERNAL.withDescription("Invalid protobuf byte sequence")
                            .withCause(e).asRuntimeException();
                } catch (IOException e) {
                    // Same for now, might be unavailable
                    throw Status.INTERNAL.withDescription("Invalid protobuf byte sequence")
                            .withCause(e).asRuntimeException();
                }
                return proto;
            }
        };
    }
}
