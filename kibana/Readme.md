## ELK와 Kibana 설치

### 1. Elasticsearch (8.11.3) 설치
```
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.11.3-x86_64.rpm
rpm --install elasticsearch-8.11.3-x86_64.rpm
```

### 2. Kibana (8.11.3) 설치
```
wget https://artifacts.elastic.co/downloads/kibana/kibana-8.11.3-x86_64.rpm
rpm --install kibana-8.11.3-x86_64.rpm
```

### 3. 서비스 시작
- 설치후 설정은 매뉴얼 통해 진행
```
systemctl start elasticsearch.service
systemctl start kibana.service
```
