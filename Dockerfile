# Use the official Python image from Dockerhub
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8080 available to the outside world
EXPOSE 8080

# Environment variable for the port
ENV PORT=8080

# Run the app
CMD ["python", "app.py"]

