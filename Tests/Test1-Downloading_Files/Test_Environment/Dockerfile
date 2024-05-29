# Use the base image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install the dependencies
RUN python3 -m pip install -r requirements.txt

# Command to run the script
CMD ["python", "main.py"]
