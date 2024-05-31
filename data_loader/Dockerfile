# Use a JDK base image
FROM openjdk:latest

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the Maven project files to the container
COPY . /usr/src/app

# Build your Maven project
RUN mvn clean install

# Define the entry point for your application
CMD ["java", "-cp", "target/classes", "ActivityProducer"]
