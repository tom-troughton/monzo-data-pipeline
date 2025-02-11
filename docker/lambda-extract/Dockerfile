# Dockerfile-extract
FROM public.ecr.aws/lambda/python:3.12

# Create app directory
WORKDIR /var/task

# Copy entire project
COPY utils/ ./utils/
COPY lambdas/extract/ ./lambdas/extract/
COPY setup.py ./

# Install the shared library as well as any local requirements
RUN pip install .
RUN pip install -r lambdas/extract/requirements.txt

# Set the CMD to your handler
CMD [ "lambdas/extract/main.lambda_handler" ]
