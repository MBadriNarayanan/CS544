# Using default Ubuntu Image
FROM ubuntu:24.04

# Using this to avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Update and installation of different packages
# Removing packages to reduce docker image size
RUN apt-get update && apt-get install -y wget grep gzip && rm -rf /var/lib/apt/lists/*

# Changing working directory
WORKDIR /p1_app

# Copying the script to the container
COPY count.sh .

# Chaning permissions for the script
RUN chmod +x count.sh

# Set the entry point to run the script
ENTRYPOINT ["./count.sh"]