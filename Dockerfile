FROM public.ecr.aws/lambda/python:3.12

# Set AWS Region
ENV AWS_DEFAULT_REGION=eu-north-1
ENV AWS_REGION=eu-north-1

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install dependencies
RUN pip install -r requirements.txt

# Copy the entire src directory
COPY src ${LAMBDA_TASK_ROOT}/src

# Create logs directory in /tmp
RUN mkdir -p /tmp/logs

# Debug: List contents to verify file structure
RUN echo "Contents of ${LAMBDA_TASK_ROOT}:" && \
    ls -la ${LAMBDA_TASK_ROOT} && \
    echo "\nContents of ${LAMBDA_TASK_ROOT}/src:" && \
    ls -la ${LAMBDA_TASK_ROOT}/src

# Add both the Lambda task root AND src directory to PYTHONPATH
ENV PYTHONPATH "${LAMBDA_TASK_ROOT}:${LAMBDA_TASK_ROOT}/src"

# Set the handler
CMD [ "src.main.lambda_handler" ]