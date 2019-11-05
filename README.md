# Sample AWS Glue script for data linking

A first attempt at a Glue job that performs data deduplication.

![image](dag.png?raw=true)

High level overview of the job (use VS Code Markdown Preview Enhanced to view).

```mermaid
graph TD
A[Ground truth dataset] -->|from fake data| B
B[Train test split] --> C
C[Tokenise records] -->|Concat all columns we're using for matching and split into array of tokens| C2
C2[Compute lookup table containing relative frequency of each token] -->D
 D[Apply Blocking rules] --> |Apply series of OR rules like 'firstname' and 'surname' or 'firstname' and 'dob' to produc|E
E[Dataset of potentially matching pairs] --> F
F[Compute features] -->G
F -->H
G[Edit distance] -->J
H[Probability score] -->|Lookup each matching token in token frequency table and multply together to produce score|J
J[Train logit model] --> K
K[Apply trained model to test data] --> L
L[Compute accuracy statistics on test data]
```

## Further details

You can find a full example with output dataframes at each stage [here](https://github.com/moj-analytical-services/data_linking_glue_job_test/blob/master/match/step_by_step_example.ipynb)