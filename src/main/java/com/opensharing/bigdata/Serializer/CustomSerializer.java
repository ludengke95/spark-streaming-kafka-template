package com.opensharing.bigdata.Serializer;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;

/**
 * @author :
 * @date : 2018/11/1.
 */
public class CustomSerializer implements ZkSerializer {

	private String charset = "UTF-8";

	public CustomSerializer() {
	}

	public CustomSerializer(String charset) {
		this.charset = charset;
	}

	@Override
	public byte[] serialize(Object data) throws ZkMarshallingError {
		try {
			return String.valueOf(data).getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			throw new ZkMarshallingError("Wrong Charset:" + charset);
		}
	}

	@Override
	public Object deserialize(byte[] bytes) throws ZkMarshallingError {
		String result;
		try {
			result = new String(bytes, charset);
		} catch (UnsupportedEncodingException e) {
			throw new ZkMarshallingError("Wrong Charset:" + charset);
		}
		return result;
	}
}
