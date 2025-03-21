# Use a Python 3.8 image as the base
FROM python:3.12.2

# Set the working directory in the container
WORKDIR /app

# Copy the application files into the container
COPY . /app

# Install the required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables for database connection (optional if needed)
ENV DB_HOST=${DB_HOST}
ENV DB_PORT=${DB_PORT}
ENV DB_NAME=${DB_NAME}
ENV DB_USER=${DB_USER}
ENV DB_PASSWORD=${DB_PASSWORD}

# Command to run the application
CMD ["python", "main.py"]
