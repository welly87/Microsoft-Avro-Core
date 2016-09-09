using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Microsoft.Hadoop.Avro
{
    public class DynamicAvroSerializer
    {
        Dictionary<Type, MethodInfo> _serializers = new Dictionary<Type, MethodInfo>();
        Dictionary<Type, MethodInfo> _deserializers = new Dictionary<Type, MethodInfo>();

        public byte[] Ser<T>(T obj)
        {
            // TODO need to cache serializer 
            var avroSerializer = AvroSerializer.Create<T>(
                new AvroSerializerSettings
                {
                    Resolver = new AvroPublicMemberContractResolver(),
                    UseCache = true
                });

            byte[] bytes = null;

            using (var memoryStream = new MemoryStream())
            {
                avroSerializer.Serialize(memoryStream, obj);
                bytes = memoryStream.ToArray();
            }

            return bytes;
        }

        public object Deser<T>(byte[] bytes)
        {
            var avroSerializer = AvroSerializer.Create<T>(
                new AvroSerializerSettings
                {
                    Resolver = new AvroPublicMemberContractResolver(),
                    UseCache = true
                });

            object obj = null;

            using (var memoryStream = new MemoryStream(bytes))
            {
                obj = avroSerializer.Deserialize(memoryStream);
                bytes = memoryStream.ToArray();
            }

            return obj;
        }

        public byte[] Serialize(object obj)
        {
            var type = obj.GetType();

            if (!_serializers.ContainsKey(type))
            {
                MethodInfo method = this.GetType().GetMethod("Ser");
                MethodInfo genericSerializer = method.MakeGenericMethod(obj.GetType());
                _serializers.Add(type, genericSerializer);
            }
            
            return _serializers[obj.GetType()].Invoke(this, new object[] { obj }) as byte[];
        }

        public object Deserialize(byte[] bytes, Type type)
        {
            if (!_deserializers.ContainsKey(type))
            {
                MethodInfo method = this.GetType().GetMethod("Deser");
                MethodInfo genericDeserializer = method.MakeGenericMethod(type);
                _deserializers.Add(type, genericDeserializer);
            }

            return _deserializers[type].Invoke(this, new object[] { bytes });
        }
    }
}
