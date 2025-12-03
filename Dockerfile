FROM eclipse-temurin:21-jdk-alpine

WORKDIR /app

# Copy your source
COPY Controller.java Dstore.java ./

# Compile both entry points
RUN javac Controller.java Dstore.java

# Default command (overridden in docker-compose for Dstores)
CMD ["java", "Controller", "5000", "2", "5000", "30"]
