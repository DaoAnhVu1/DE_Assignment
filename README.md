# Build a "simple" data platform

### *Step 1: Set up docker containers*
Run the command ```docker compose up```, this should spin up all the required containers

### *Step 2: Produce records into Kafka*
For this setup, Kafka will be available at:
 -  ```localhost:9094``` if you are using your personal computer
 - ```broker01:9092``` if you want to access it through another container

In this case, I will produce messages using my personal computer, and the code is located within the ```activity_producer``` folder. This is a typical Maven project, just run the ActivityProducer main method and you should be ready to go.

### *Step 3: Ingest data using Nifi*
After producing the records into Kafka, we are ready to ingest it using the Nifi container. The username and password are provided in the ```docker-compose.yml```. Just access it in ```localhost:8443/nifi```. 
- After sucessfully log into Nifi, we need to upload a template, because I did not find a way to save Nifi Flowfile :(. It is located at ```nifi_template``` folder. Right click on the blank canvas and chose upload template.
- After that, make sure to click configure and enable all the required controller before starting.
- Add a processor to convert the consumed Avro format into your desired format.