# Hebrew Word Prediction System

### Submitted by:

- **Name:** Yarden Greenpeter
- **ID:** 318517653
- **Name:** Tamar Cohen
- **ID:** 315218073

### Project Overview

This project generates a **knowledge-base** for a **Hebrew word-prediction system** based on the **Google 3-Gram Hebrew dataset**.

---

## How to Run?

### Steps:

1. **Create JAR File**  
   Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.

2. **Run the Program**  
   Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.

   ```bash
   java -jar target/WordPredictionSystemLorem ipsum.jar input_file output_file
   ```

---

## Key Sections of the Project

### 1. Key-Value Pairs Sent from Mappers to Reducers

- **With Local Aggregation**  
  Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.

- **Without Local Aggregation**  
  Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.

### 2. Scalability Report

- **Running Time with Different Numbers of Mappers**
  Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.

- **Running Time with Different Input Sizes**  
  Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.

### 3. Word Pairs Analysis

- **Choose 10 Interesting Word Pairs**  
  Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.

- **Top-5 Next Words**  
  Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.

- **Judgment of Predictions**  
  Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer nec odio. Praesent libero. Sed cursus ante dapibus diam.

---

## Implementation Notes

### Map-Reduce Steps

**1st step - CalcVariablesStep**  
 This step processes 3-grams, counts their occurrences, and performs local aggregation in the mapper. It filters out stop words and emits keys like individual words, word pairs, and trigrams with their counts

**2nd step - valuesJoinerStep**  
 In this step, the Mapper categorizes the data by splitting the keys and assigning them a specific label like "Single" or "Double" based on the key format. The Reducer aggregates these values by updating counts and combining related data. It then outputs updated key-value pairs, where the count is added to specific value fields based on the label type

**3rd step - probabilityCalcStep**  
 In this step, the Mapper simply passes the key-value pairs from the previous step without any modification. The Reducer, however, processes the values by extracting specific integers based on predefined categories ("N1:", "N2:", "C1:", "C2:") and then calculates a probability value using the Calc.calcP() method. The result of this calculation is written as a new key-value pair, where the key remains the same as the input and the value is a DoubleWritable containing the calculated probability.

**4th step - trigramListStep**  
 This step processes input key-value pairs where the key is a trigram (w1, w2, w3) and the value is a DOUBLE representing the conditional probability P(w3|w1, w2). The output is a sorted list of word trigrams and their corresponding probabilities. The sorting is done in two stages: first by the combination of w1 and w2 in ascending order, and then by the conditional probability for w3 in descending order.

---
