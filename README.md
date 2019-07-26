# Using Micro-collections in Social Media to Generate Seeds for Web Archive Collections
Code and data associated with: [ACM/IEEE-CS Joint Conference on Digital Libraries (JCDL 2019) full paper]() and [tech report](https://arxiv.org/abs/1905.12220).

## Brief description of [Code](./Code)
* MicroCols.py: Generates collections of URIs from the different social media sites.
* SegmentCols.py: Labels collections of URIs with post classes.
* PrecEval.py: Performs precision evaluation with reference gold standard.
* cdSegmentCols.py: Performs [carbondating](http://cd.cs.odu.edu/) (creation date estimation) of links.
* [genericCommon.py](https://github.com/anwala/Util): consists of utility functions used by all previously described scripts.
## Brief description of [Data](./Data)
In the dataset, the post classes were SS, MS, SM, and MM. In the paper, we used P_1A_1, P_nA_1, P_1A_n, and P_nA_n:
* Single Post, Single Author = SS = P_1A_1
* Single Post, Single Author = MS = P_nA_1
* Single Post, Single Author = SM = P_1A_n
* Single Post, Single Author = MM = P_nA_n
* MC (Micro-Collections) = P_nA_1 ∪ P_1A_n ∪ P_nA_n

The dataset topics:
1. Ebola Virus Outbreak
2. Flint Water Crisis
3. MSD Shooting
4. 2018 World Cup
5. 2018 Midterm Elections

Collection of URIs were labeled with one of the four post post class labels.
### Description of a sample of fields of single JSON file (e.g., [reddit ebola virus outbreak](./Data/ebola-virus-outbreak/reddit-0.json.gz))
- **name**: social media name - index number (string)
- **extraction-timestamp**: datetime entire collection was created (string)
- **segmented-cols**:  (array[objects])
    - (object)
        - **ss or sm or ms or mm or mc**: post classes (array[objects])
            - (object) 
                - **timestamp**: datetime when URIs were extracted (string)
                - **sim-coeff**: if predicted-precision ≥ sim-coeff, uris are labeled relevant (float)
                - **predicted-precision**: precision score of uris (float)
                - **uris**: collection of uris extracted from social media sources  (array[object])
                    - (object)
