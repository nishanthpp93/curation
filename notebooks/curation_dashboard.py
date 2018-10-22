# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 2
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython2
#     version: 2.7.12
# ---

# %matplotlib inline
import google.datalab.bigquery as bq
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

# # Gender By Race

def gender_by_race(dataset_id):
    q = bq.Query('''
    SELECT 
     c1.concept_name AS gender,
     c2.concept_name AS race,
     COUNT(1) AS `count`
    FROM `{dataset_id}.person` p
    JOIN `vocabulary20180104.concept` c1 
      ON p.gender_concept_id = c1.concept_id
    JOIN `vocabulary20180104.concept` c2
      ON p.race_concept_id = c2.concept_id
    GROUP BY c2.concept_name, c1.concept_name
    '''.format(dataset_id=dataset_id))
    df = q.execute(output_options=bq.QueryOutput.dataframe()).result()
    df['race'] = df['race'].astype('category')
    df['gender'] = df['gender'].astype('category')
    g = sns.FacetGrid(df, col='race', hue='gender', col_wrap=5)
    g.map(sns.barplot, 'gender', 'count', ci=None)
    g.set_xticklabels([])
    g.set_axis_labels('', '')
    g.add_legend()

# ## RDR

gender_by_race('rdr20180620')

# ## EHR

gender_by_race('unioned_ehr20180822')

gender_by_race('combined20180822')
