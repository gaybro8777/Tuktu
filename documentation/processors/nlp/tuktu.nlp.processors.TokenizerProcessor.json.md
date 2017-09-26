### tuktu.nlp.processors.TokenizerProcessor
Tokenizes a piece of data in a given field.

  * **id** *(type: string)* `[Required]`

  * **result** *(type: string)* `[Required]`

  * **config** *(type: object)* `[Required]`

    * **field** *(type: string)* `[Required]`
    - The field to be tokenized. The field's content must be a String or a JsString.

    * **as_string** *(type: boolean)* `[Optional, default = false]`
    - If false, an Array of Tokens (Strings) is returned; otherwise a String is returned where the tokens are separated by blanks (' ').

    * **language** *(type: string)* `[Optional]`
    - If given, a language-specific tokenizer may be applied if available. Will fall back to default if this field is not given or if the language-specific tokenizer is unavailable.

