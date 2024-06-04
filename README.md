# Build a "simple" data platform

### *Step 1: Set up docker containers*
Run the command ```docker compose up```, this should spin up all the required containers

### *Step 2: Produce records into Kafka*
For this setup, Kafka will be available at port:
 -  ```localhost:9094``` if you are using your personal computer
 - ```broker01:9092``` if you want to access it through another container

In this case, I will produce messages using my personal computer, and the code is located within the ```activity_producer``` folder. This is a typical Maven project, just run the ActivityProducer main method and you should be ready to go. The program read the data from resources.log_action.csv.

#### *Side note for the kafka producer*
The program uses serialization with Avro. The ```pom.xml``` specifies all the required packages. Every time you create a new class or schema, follow the ```.avsc``` file syntax to create it. Then, run ```mvn generate-sources```, and the class will be automatically created for you. Use the provided builder to create an instance.

### *Step 3: Ingest data using Nifi*
After producing the records into Kafka, we are ready to ingest it using the Nifi container. The username and password are provided in the ```docker-compose.yml```. Just access it in ```localhost:8443/nifi```. 
- After sucessfully log into Nifi, we need to upload a template, because I did not find a way to save Nifi Flowfile :(. It is located at ```nifi_template``` folder. Right click on the blank canvas and chose upload template. The ```DE_Assignment.xml``` wil contains all the flow necessary. 
- Drag the template into the canvas
- Make sure to click configure and enable all the required controllers/services before starting.
- The first flow with get file and put hdfs will put the ```danh_sach_sv_de.csv``` into the HDFS
- The second one will consume data from Kafka, with the configuration to enable it to reach the schema registry to know the ```Activity.avsc```, then convert it to Parquet and store it in HDFS.
### *Step 4: Data processing*
Finally, we processcing data using Apache Spark, specifically PySpark. the code are located at spark-notebook, the output will be located at spark-notebooks/output.