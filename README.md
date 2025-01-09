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

1. **Update Configuration Files**  
   Before creating the JAR file, make sure to update the configuration in **Defs.java**. Modify the following variables as needed:

   - `public static String stopWordsFile = "...../heb-stopwords.txt";`
   - `public static final String PATH_TO_TARGET = "...../target/";`

   Additionally, adjust the settings according to your requirements:

   - `public static boolean localAggregationCommand = true/false;`
   - `public static final int instanceCount = (int);`
   - `public static final String PROJECT_NAME = ".....";`

2. **Create JAR File**  
   To create the JAR file, run the following command:

   ```bash
   mvn clean package -P aws-hadoop-setup,CalcVariablesStep,valuesJoinerStep,probabilityCalcStep,trigramListStep
   ```

   This will generate the necessary JAR files.

3. **Run the Program**  
   Once the JAR file is created, you can run the program using this command:

   ```bash
   java -jar ".....\target\aws-hadoop-setup.jar"
   ```

Make sure to replace `.....` with the actual path to your project directory.

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

**10 Interesting Word Pairs and Top-5 Next Words**

---

- ארצות הברית

  - ארצות הברית ובריטניה 0.012383910646205806
  - ארצות הברית וברית 0.011624292765366214
  - ארצות הברית בישראל 0.011531516088483074
  - ארצות הברית וקנדה 0.007854256053384153
  - ארצות הברית למלחמה 0.007282157453090195

    The probabilities show common collocations with other countries (Britain, Canadaת Israel) and contexts of international relations (war). The low probabilities (0.7-1.2%) suggest high variability in the third word, which is reasonable given the diverse contexts in which the US is discussed.

---

- השקפת העולם

  - השקפת העולם הדתית 0.09752293612114937
  - השקפת העולם היהודית 0.07456167687991548
  - השקפת העולם שלו 0.04323329249553334
  - השקפת העולם הציונית 0.0387908883651783
  - השקפת העולם הסוציאליסטית 0.032837370592245056

    The high probabilities for religious (9.7%) and Jewish (7.4%) worldviews highlight the strong influence of religious and cultural discussions. The spread of various ideological perspectives (religious, Jewish, Zionist, socialist) seems to align well with historical and cultural contexts.

---

- כותב רבי

  - כותב רבי חיים 0.09437596267484803
  - כותב רבי אברהם 0.0759814684273206
  - כותב רבי משה 0.06882606036852768
  - כותב רבי יוסף 0.06862005824556114
  - כותב רבי יעקב 0.0510053232944076

    The probabilities follow expected patterns of rabbinic name frequency.

---

- בשביל שלא

  - בשביל שלא הוכיחו 0.05054563635123882
  - בשביל שלא יהיה 0.03506014676230102
  - בשביל שלא יהא 0.020071300573781786
  - בשביל שלא שינו 0.019911951766013514
  - בשביל שלא רצה 0.01771562931436165

    The probabilities reflect typical Hebrew grammar patterns. The highest probability (5%) for "hochihu" (proved) indicates its frequent use in argumentative or explanatory contexts.

---

- חיים לאומיים

  - חיים לאומיים שלמים 0.08915292714009394
  - חיים לאומיים עצמאיים 0.0696336037288976
  - חיים לאומיים מלאים 0.058930094140824354
  - חיים לאומיים חדשים 0.04226354986775462
  - חיים לאומיים עצמיים 0.04072058207304552

    The strong association with adjectives like completeness (8.9% for "shlemim") and independence (6.9% for "atzma'im") highlights frequent discussions on national sovereignty and growth.

---

- טוב מאוד

  - טוב מאוד טוב 0.014408221423177738
  - טוב מאוד גם 0.013997592047528085
  - טוב מאד מאד 0.012915728840680256
  - טוב מאד והנה 0.011100022719710435
  - טוב מאד ויהי 0.007617388182225383

    Low probabilities (0.7-1.4%) indicate high variability in usage, which is expected for this common phrase.

---

- ידעתי שאני

  - ידעתי שאני רוצה 0.08038387145649827
  - ידעתי שאני צריך 0.06421237721060757
  - ידעתי שאני חייב 0.06327524248652441
  - ידעתי שאני צריכה 0.047447015623313334
  - ידעתי שאני יכול 0.047307568673595396

    Strong collocations with modal verbs (want, need, must) show typical patterns of self-reflection. The probabilities (4.7-8%) seem appropriate for personal narrative contexts.

---

- יהודים ממוצא

  - יהודים ממוצא ספרדי 0.13175773669998178
  - יהודים ממוצא גרמני 0.11249626561514856
  - יהודים ממוצא מזרח 0.09626534140509765
  - יהודים ממוצא פולני 0.06501618912871802
  - יהודים ממוצא מזרחי 0.056509920249011664

    High probabilities for specific ethnic origins (Sephardic 13.1%, German 11.2%) reflect historical Jewish demographics and common discourse about Jewish ethnic groups.

---

- מכאן ניתן

  - מכאן ניתן להסיק 0.23132838301327927
  - מכאן ניתן ללמוד 0.14030731727181092
  - מכאן ניתן להבין 0.09219930016535548
  - מכאן ניתן לשער 0.03293454592838589
  - מכאן ניתן לראות 0.028153505883374768

    Very high probability (23.1%) for "lehesik" (to conclude) shows strong grammatical patterning. The distribution across similar verbs (learn, understand, estimate) reflects typical academic or analytical discourse.

---

- בעבר הירדן

  - בעבר הירדן המזרחי 0.19102590790058102
  - בעבר הירדן מזרחה 0.10322751128297603
  - בעבר הירדן המערבי 0.043156901075326694
  - בעבר הירדן בארץ 0.025372879915737025
  - בעבר הירדן מערבה 0.021939024083021407

    High probability (19.1%) for "hamizrachi" (the eastern) reflects the common historical-geographical usage. The distribution between eastern and western qualifiers matches the geographical reality.

---

Overall, the probabilities appear to reflect natural language patterns in Hebrew, with higher probabilities for fixed phrases and technical terms, and lower probabilities for more general expressions. The system seems to capture both grammatical patterns and cultural-historical contexts effectively.

## Implementation Notes

### Map-Reduce Steps

- **1st step - CalcVariablesStep**  
  This step processes 3-grams, counts their occurrences, and performs local aggregation in the mapper. It filters out stop words and emits keys like individual words, word pairs, and trigrams with their counts

- **2nd step - valuesJoinerStep**  
  In this step, the Mapper categorizes the data by splitting the keys and assigning them a specific label like "Single" or "Double" based on the key format. The Reducer aggregates these values by updating counts and combining related data. It then outputs updated key-value pairs, where the count is added to specific value fields based on the label type

- **3rd step - probabilityCalcStep**  
  In this step, the Mapper simply passes the key-value pairs from the previous step without any modification. The Reducer, however, processes the values by extracting specific integers based on predefined categories ("N1:", "N2:", "C1:", "C2:") and then calculates a probability value using the Calc.calcP() method. The result of this calculation is written as a new key-value pair, where the key remains the same as the input and the value is a DoubleWritable containing the calculated probability.

- **4th step - trigramListStep**  
  This step processes input key-value pairs where the key is a trigram (w1, w2, w3) and the value is a DOUBLE representing the conditional probability P(w3|w1, w2). The output is a sorted list of word trigrams and their corresponding probabilities. The sorting is done in two stages: first by the combination of w1 and w2 in ascending order, and then by the conditional probability for w3 in descending order.

---
