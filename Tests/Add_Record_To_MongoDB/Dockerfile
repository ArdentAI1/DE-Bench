FROM python:3.9-alpine

# Create and set the working directory
WORKDIR /app

# Copy the environment file
COPY Test_Environment /app

# The tail command keeps the container running. You can replace it with your actual command if needed.
CMD ["tail", "-f", "/dev/null"]