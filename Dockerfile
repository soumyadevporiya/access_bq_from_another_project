FROM python:3.9
WORKDIR ./
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt
COPY ./access_bq_from_another_project.py ./access_bq_from_another_project.py
CMD ["python3","./access_bq_from_another_project.py"]
