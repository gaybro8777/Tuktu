### tuktu.nlp.processors.Word2VecAverageWordsVector
Computes an average document vector for word vectors in a sentence.

  * **id** *(type: string)* `[Required]`

  * **result** *(type: string)* `[Required]`

  * **config** *(type: object)* `[Required]`

    * **model_name** *(type: string)* `[Required]`
    - Name of the model to be applied. If a model with this name cannot be found, the data will go through unchanged.

    * **destroy_on_eof** *(type: boolean)* `[Optional, default = true]`
    - Will this model be cleaned up once EOF is reached.

    * **data_field** *(type: string)* `[Required]`
    - The field the data resides in. Data can be textual (String) or Seq[String].

    * **tfidf_field** *(type: string)* `[Optional]`
    - An optional field containing TF-IDF scores that weight the word vectors.

