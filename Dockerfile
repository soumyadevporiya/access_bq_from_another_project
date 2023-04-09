FROM python:3.9
WORKDIR ./
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt
COPY ./ns_monolith_parallel_reader.py ./ns_monolith_parallel_reader.py
CMD ["python3","./ns_monolith_parallel_reader.py"]
