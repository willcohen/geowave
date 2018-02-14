package mil.nga.giat.geowave.analytic.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public class PersistableSerializer extends
		Serializer<Persistable>
{

	@Override
	public Persistable read(
			Kryo kryo,
			Input input,
			Class<Persistable> classTag ) {
		
		//Read the class from input stream
		Registration inputReg = kryo.readClass(input);
		

		//Create instance of persistable concrete child type and cast to persistable
		Object newObj = kryo.newInstance(inputReg.getType());
		
		
		Persistable returnObj = classTag.cast(newObj);
		
		//Make sure kryo references that object for reading
		kryo.reference(returnObj);
		
		//Read object byte count and allocate buffer to read object data
		int byteCount = input.readInt();
		byte[] bytes = new byte[byteCount];
		input.read(bytes);
		
		//Take bytebuffer and initialize object from bytes
		returnObj.fromBinary(bytes);
		return returnObj;
	}

	@Override
	public void write(
			Kryo kryo,
			Output output,
			Persistable object ) {
		
		//Write class so we know what class to instantiate
		kryo.writeClass(output, object.getClass());
		
		//Get bytes representing object and write byteCount + bytes
		byte[] objBuffer = object.toBinary();
		output.writeInt(objBuffer.length);
		output.write(objBuffer);
	}

}