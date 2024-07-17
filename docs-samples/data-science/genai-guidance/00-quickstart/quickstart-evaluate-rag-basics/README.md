# Evaluation of RAG Performance Basics: A Step-by-Step Guide

This tutorial provides a quickstart guide to use Fabric for evaluating the performance RAG applications. Performance evaluation is focused on two main components of RAG: the retriever (in our scenario it is based on Azure AI Search) and response generator (an LLM that uses provided user query, retrieved context, and prompt to spit out a reply that can be served to the user). The main steps in this tutorial are as following:

1. Set up Azure OpenAI and Azure AI Search Services
2. Load and manipulate the data from CMU's QA dataset of Wikipedia articles to curate a benchmark
3. Run a "smoke" test with one query to confirm that the RAG system works end-to-end
4. Define deterministic and AI-Assisted metrics that will be used for evaluation
5. Check-in #1 - evaluate the performance of retriever using "top-N accuracy score"
6. Check-in #2 - evaluate the performance of response generator using Groundedness, Relevance, and Similarity metrics
7. Visualize results and preserve them in OneLake for future reference and continuous evaluation


<img src="https://appliedaipublicdata.blob.core.windows.net/cmuqa-08-09/output/rag_flow_evaluation_basics.png" style="width:1000px;"/>
 
## Prerequisites

‼️ Prior to starting this tutorial, please complete "Building Retrieval Augmented Generation in Fabric: A Step-by-Step Guide" [here](https://github.com/microsoft/fabric-samples/blob/main/docs-samples/data-science/genai-guidance/00-quickstart/quickstart-bring-your-own-keys/quickstart-genai-guidance.ipynb)

You need the following services to run this notebook.

- [Microsoft Fabric](https://aka.ms/fabric/getting-started)
- [Add a lakehouse](https://aka.ms/fabric/addlakehouse) to this notebook (it should have data populated with steps from previous tutorial).
- [Azure AI Studio for OpenAI](https://aka.ms/what-is-ai-studio)
- [Azure AI Search](https://aka.ms/azure-ai-search) (it should have data populated with steps from previous tutorial).

In the previous tutorial, you have uploaded data to your lakehouse and built an index of documents that is in the backend of RAG. The index will be used here as part of an exercise to learn the main techniques for evaluation of RAG performance and identification of potential problems. If you haven't done this yet, or removed previously created index, please follow [here](https://github.com/microsoft/fabric-samples/blob/main/docs-samples/data-science/genai-guidance/00-quickstart/quickstart-bring-your-own-keys/quickstart-genai-guidance.ipynb) to complete that prerequisite.

## Deployment Instructions

Follow these instructions [to import a notebook into Fabric](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook). After uploading the notebook, you will need to ensure you have the following to insert into a cell for the remainder of the code to work.

- OpenAI endpoint and keys
- Azure AI Search endpoint and keys

Make sure to use the `environment.yaml` to upload into Fabric to create, save, and publish a [Fabric environment](https://learn.microsoft.com/en-us/fabric/data-engineering/create-and-use-environment). 

Then select the newly created environment before running the notebook. The very last cell of the notebook display an ipywidget with a dialogue box for a user to write their question and a response will be given. If the user wishes to ask a new question, simply change the text in the question and press Enter. 
 
## Modified CMU Question/Answer Dataset

Considering the original dataset has different licenses for S08/S09 and S10, the modified dataset has been created of only S08/S09 rows with a reference to the ExtractedPath. For simplicity, the data is cleaned up and refined into a single structured table with the following fields.

- ArticleTitle: the name of the Wikipedia article from which questions and answers initially came.
- Question: manually generated question based on article
- Answer: manually generated answer based on question and article
- DifficultyFromQuestioner: prescribed difficulty rating for the question as given to the question-writer
- DiffuctlyFromAnswerer: Difficulty rating assigned by the individual who evaluated and answered the question, which may differ from the difficulty from DifficultyFromQuestioner
- ExtractedPath: path to original article. There may be more than one Question-Answer pair per article
- text: cleaned wikipedia article

### History 
CMU Question/Answer Dataset, Release 1.2

8/23/2013

Noah A. Smith, Michael Heilman, and Rebecca Hw

Question Generation as a Competitive Undergraduate Course Project

In Proceedings of the NSF Workshop on the Question Generation Shared Task and Evaluation Challenge, Arlington, VA, September 2008. 
Available at: http://www.cs.cmu.edu/~nasmith/papers/smith+heilman+hwa.nsf08.pdf

Original dataset acknowledgements:

This research project was supported by NSF IIS-0713265 (to Smith), an NSF Graduate Research Fellowship (to Heilman), NSF IIS-0712810 and IIS-0745914 (to Hwa), and Institute of Education Sciences, U.S. Department of Education R305B040063 (to Carnegie Mellon).

cmu-qa-08-09 (modified verison)

6/12/2024

Amir Jafari, Alexandra Savelieva, Brice Chung, Hossein Khadivi Heris, Journey McDowell

Released under same license GFDL (http://www.gnu.org/licenses/fdl.html)
All the GNU license applies to the dataset in all copies.