# Community Samples

Some of our community samples have produced great samples that show off the capabilities of Microsoft Fabric. We have collected them here for you to use and learn from.

> ⚠️ **Note:** These samples are provided 'as-is' and are not officially supported. 
> Please consider this code as any code you find on Internet and **proceed with caution before running it** on your environement.
> If you have any questions, please don't hesitate to ask on the [Microsoft Fabric Community](https://community.fabric.microsoft.com/).

## Index

| Sample | Workloads | Short Description | GitHub | Video |
|--------|-----------|-------------------|--------|-------|


## Samples

### ⚠️⚠️ PIInovators

PIInovators is a cloud-native data solution developed in Microsoft Fabric, integrating OpenAI for document analysis, particularly for detecting Personal Identification Information (PII) in files and images. The solution classifies documents into compliant and non-compliant categories, further categorizing them based on predefined types (Delivery, Personnel, Online, Continual, Communication). The solution is based on a medallion architecture where data is stored in three zones (Bronze, Silver, Gold) within Microsoft Fabric Lakehouse. Subsequently, the data is prepared for analytical use in Power BI reports.

- Project Repository: https://github.com/esddata/piinovators.git
- Video: https://youtu.be/vvfRFNgo-PI

### The Foodwaste Hack

We want to show you how you can combine Powerapps, Fabric and AI to minimise food waste in your kitchen.

Ever looked into your fridge and thought “darn, should have cooked that chicken sooner?” Worry no more. we have a hack for you.

In our github repo you will find everything you need to create a power app to help track food you have bought, assisted by your own lakehouse with millions of product descriptions, use AI to help with setting expiration dates, and again use AI to recommend some yummy recipe ideas on what to do with your ingredients. The best part? A reflex trigger in Fabric will know when your items are about to go off and automatically trigger your own master chef to suggest some great recipe ideas and email them to you before it is too late!

- Project Repository: https://github.com/AllgeierSchweiz/aihackers
- Video: https://youtu.be/hgCOdZzBuq4

### ⚠️⚠️ Building an Analytics Solution over a Text Document Repository

This project leverages the power of Microsoft Fabric and Azure OpenAI to enrich and analyze a repository of text-based data. So many companies have valuable unstructured data that can now be unlocked with Azure OpenAI. In this project, Azure OpenAI enriches the data by performing entity extraction, text summarization, text classification, and embedding generation for semantic similarity. The end result is an analytics solution with Notebooks and Power BI over the document repository that can be used by both developers and business users!

- Project Repository: https://github.com/BrightonKahrs/project-gutenberg-analysis/tree/main
- Video: https://youtu.be/zfkZxRUkEn8

### Accounts Payable Payment Prediction

Our project uses Accounts Payable data from an ERP system to predict the payment status of unpaid invoices – specifically whether they will be paid early, late, or on-time. This data in moved through a “medallion” lakehouse architecture and modeled in various notebooks. The results of the predictive model are presented alongside traditional AP reporting in Power BI. Additionally, we generated our calendar table and semantic model descriptions using Copilot. This project is designed to fulfill a real-world need and will be further developed into a production-ready solution for our organization. Thanks for considering our submission!

- Project Repository: https://github.com/aboerger/Fabric-Hackathon-AP-Payment-Prediction
- Video: https://youtu.be/RI7mAwF30wY

### Fabric SQL DB audit

The solution proposed involves the collection of SQL audit logs and their near real-time streaming in a simple way, into Microsoft Fabric thanks to its simplicity

This efficient process ensures that logs are stored systematically and securely.
Furthermore, the solution offers additional capabilities that are critical for data analysis and management. Thanks to Fabric’s capabilities, include the ability to analyze audit events with LLM and the compatibility with various data scientist tools.

This solution, therefore, not only streamlines the data collection process but also enhances data utility, making it a valuable addition to any data management strategy.

- Project Repository: https://github.com/gianlucadardia/FabricHack.git
- Video: https://github.com/gianlucadardia/FabricHack/blob/main/video/FabricHack.mp4

### Automating synthetic data creation & reporting using Microsoft Fabric

When considering privacy protection in the context of an outsourcing company, synthetic data generation becomes particularly relevant due to the sensitive nature of the data involved. Outsourcing companies often handle data from clients that may contain personally identifiable information (PII), financial records, proprietary business information, and other sensitive data.

By leveraging synthetic data generation techniques, outsourcing companies can effectively manage and mitigate the privacy risks associated with handling sensitive data, thereby building trust with clients, enhancing data security, and ensuring regulatory compliance.

- Project Repository: https://github.com/corneliascode/Automating-synthetic-data-creation-reporting-using-Microsoft-Fabric-
- Video: https://youtu.be/2XKAw3h9W0Y