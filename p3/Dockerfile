FROM ubuntu:24.04

# Using this to avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Installing the required packages
RUN apt-get update && apt-get install -y python3-pip

# Set the working directory to root
WORKDIR /

# Copy the required files directly to root
COPY bigdata.py /
COPY csvsum.py /
COPY parquetsum.py /
COPY upload.py /
COPY simple.csv /
COPY table.proto /
COPY server.py /
COPY requirements.txt /

# Change permissions for the files
RUN chmod +x bigdata.py csvsum.py parquetsum.py upload.py simple.csv
RUN chmod +x server.py table.proto requirements.txt

# Install dependencies
RUN pip3 install -r requirements.txt --break-system-packages

# Generate the gRPC stubs
RUN python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. table.proto

# Expose the gRPC port
EXPOSE 5440

# Run the gRPC server
CMD ["python3", "/server.py"]