# Set base image to PostgreSQL with matching client version
# Docker-compose will build the container based on this image
FROM python:3.9.13

# Set working directory
WORKDIR /workspace

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install necessary dependencies including Docker CLI
RUN apt-get update && \
    apt-get install -y \
        postgresql-client \
        curl \
        gnupg2 \
        lsb-release

# Add Docker's official GPG key
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -

# Set up the stable repository for Docker
RUN echo "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list

# Install Docker CLI (This allows docker commands to be used from within the container's terminal)
RUN apt-get update && \
    apt-get install -y docker-ce-cli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install ipykernel
RUN pip install --no-cache-dir ipykernel

# Add Python kernel to Jupyter
RUN python -m ipykernel install --user --name python_postgresql --display-name "Python PostgreSQL"

# Install Jupyter extension for VSCode
RUN /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/microsoft/vscode-dev-containers/master/script-library/jupyter.sh)"

# Copy requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .