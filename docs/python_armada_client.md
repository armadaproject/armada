---
docname: python_armada_client
images: {}
path: /python-armada-client
title: armada_client package
---

# armada_client package

## armada_client.client module

## armada_client.event module


### _class_ armada_client.event.Event(event)
Represents a gRPC proto event

Definition can be found at:
[https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284](https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284)


* **Parameters**

    **event** (*armada.event_pb2.EventStreamMessage*) – The gRPC proto event


## armada_client.permissions module


### _class_ armada_client.permissions.Permissions(subjects, verbs)
Permissions including Subjects and Verbs

```python
permissions = Permissions(...)
client = ArmadaClient(...)

queue = client.create_queue(
    permissions=[permissions],
)
```


* **Parameters**

    
    * **subjects** (*List**[**armada_client.permissions.Subject**]*) – 


    * **verbs** (*List**[**str**]*) – 



#### to_grpc()
Convert to grpc object


* **Return type**

    armada.submit_pb2.Permissions



### _namedtuple_ armada_client.permissions.Subject(kind, name)
Subject is a NamedTuple that represents a subject in the permission system.


* **Fields**

    
    1.  **kind** (`str`) – Alias for field number 0


    2.  **name** (`str`) – Alias for field number 1



* **Parameters**

    
    * **kind** (*str*) – 


    * **name** (*str*) – 



#### to_grpc()
Convert this Subject to a grpc Subject.


* **Return type**

    armada.submit_pb2.Subject
