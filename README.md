# Real-Time-Fraud-Detection-System
Project is based on event-driven architecture, real-time stream processing, and microservices design in production grade environment.

My Goals for project are :
1. project capacity upto 100k+ per sec 
2. 400ms for transaction to fraud detection. 

Technologies used : Python, Go, Node.js, Docker, Kubernetes, Kafka, PostgreSQL, Redis .

System architecture followed : 


<img width="663" height="585" alt="Screenshot 2026-03-07 at 1 57 10 AM" src="https://github.com/user-attachments/assets/a7b105dc-c975-45b1-8363-05d91042cfb3" />


Steps followed by me to implement this end to end : 

1. Project setup and Docker setup
2. Database schema design
3. Kafka setup - clusters and topics
4. Services :
       a. Transaction processor
       b. Feature enigine
       c. currently working on 



Goal : Latency timeline expected :

<img width="1283" height="117" alt="Screenshot 2026-03-07 at 2 10 08 AM" src="https://github.com/user-attachments/assets/956108f1-64dd-417b-8033-b993299b6a03" />


Can be scaled using combination of : Kafka partitioning + stateless microservices + Kubernetes HPA + databases

Diagram explanation of project : 

Customer Makes Payment
↓

Kafka (Event Hub) ←  All transactions flow here
↓

Transaction Processor ← Validates data

↓

Feature Engine ← "Is this user normally in Arizona?"

↓

Model Service ← ML model: "Fraud risk = 72%"

↓

Decision Engine ← Apply rules: "70% risk = REVIEW"

↓

Alert Service ← Tell merchant & customer

↓

Database ← Store everything (for audit trail)
