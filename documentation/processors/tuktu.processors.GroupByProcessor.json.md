### tuktu.processors.GroupByProcessor
Groups data by the value list of the provided fields.

  * **id** *(type: string)* `[Required]`

  * **result** *(type: string)* `[Required]`

  * **config** *(type: object)* `[Required]`

    * **fields** *(type: array)* `[Required]`
    - The fields to group on. First field will be used as root-grouping, then the next field etc.

      * **[UNNAMED]** *(type: string)* `[Required]`

    * **sync** *(type: boolean)* `[Optional, default = false]`
    - Whether or not the result of the remaining flow is required to be send back.

