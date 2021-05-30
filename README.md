## **CouchDB processor for Nifi.**

It is a processor programmed 100% in Java. It is able to store a document in a CouchDB database. Within the NiFi tool you can connect it to a GenerateFlowfiles processor and pass it the documents you want to save.

To obtain the processor it is necessary to execute the command `mvn clean install` in the principal folder.
Then You have to add the nar archive obtained to the NiFi's lib folder.

Anyone is free to take the code from this repository and use it.

Authors: Agustín Pallarés García and Polytechnic University of Madrid
