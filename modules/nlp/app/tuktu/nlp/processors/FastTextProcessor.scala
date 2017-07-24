package tuktu.nlp.processors

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.ops.transforms.Transforms

import play.api.libs.iteratee.Enumeratee
import play.api.libs.json.JsObject
import tuktu.api.BaseProcessor
import tuktu.api.DataPacket
import tuktu.api.utils
import tuktu.nlp.models.FastTextCache
import tuktu.nlp.models.FastTextWrapper
import play.api.libs.json.JsArray
import play.api.libs.json.Json

class FastTextProcessor(resultName: String) extends BaseProcessor(resultName) {
    //val models = collection.mutable.Map.empty[String, FastTextWrapper]
    var modelName: String = _
    var tokensField: String = _
    
    /*var lr: Double = _
    var lrUpdateRate: Int = _
    var dim: Int = _
    var ws: Int = _
    var epoch: Int = _
    var minCount: Int = _
    var minCountLabel: Int = _
    var neg: Int = _
    var wordNgrams: Int = _
    var lossName: String = _
    var ftModelName: String = _
    var bucket: Int = _
    var minn: Int = _
    var maxn: Int = _
    var thread: Int = _
    var t: Double = _
    var label: String = _
    var pretrainedVectors: String = _*/
    
    override def initialize(config: JsObject) {
        /*// Get all relevant parameters, with the defaults used by fastText
        lr = (config \ "learn_rate").asOpt[Double].getOrElse(0.05)
        lrUpdateRate = (config \ "learn_rate_update_rate").asOpt[Int].getOrElse(100)
        dim = (config \ "vector_size").asOpt[Int].getOrElse(100)
        ws = (config \ "window_size").asOpt[Int].getOrElse(5)
        epoch = (config \ "epochs").asOpt[Int].getOrElse(5)
        minCount = (config \ "min_count").asOpt[Int].getOrElse(5)
        minCountLabel = (config \ "min_count_label").asOpt[Int].getOrElse(0)
        neg = (config \ "negative").asOpt[Int].getOrElse(5)
        wordNgrams = (config \ "word_n_grams").asOpt[Int].getOrElse(1)
        lossName = (config \ "loss_name").asOpt[String].getOrElse("ns")
        ftModelName = (config \ "ft_model_name").asOpt[String].getOrElse("sg")
        bucket = (config \ "buckets").asOpt[Int].getOrElse(2000000)
        minn = (config \ "min_n_gram").asOpt[Int].getOrElse(3)
        maxn = (config \ "max_n_gram").asOpt[Int].getOrElse(6)
        thread = (config \ "threads").asOpt[Int].getOrElse(1)
        t = (config \ "sampling_threshold").asOpt[Double].getOrElse(1e-4)
        label = (config \ "label_prefix").asOpt[String].getOrElse("__label__")
        pretrainedVectors = (config \ "pretrained_vectors_file").asOpt[String].getOrElse("")*/
        
        modelName = (config \ "model_name").as[String]
        tokensField = (config \ "tokens_field").as[String]
    }
    
    override def processor(): Enumeratee[DataPacket, DataPacket] = Enumeratee.mapM((data: DataPacket) => Future {
        new DataPacket(data.data.map {datum =>
            // See if we need to load a new model
            val newModelName = utils.evaluateTuktuString(modelName, datum)
            
            /*val pretrainedFile = utils.evaluateTuktuString(pretrainedVectors, datum)
            if (!models.contains(newModelName)) { 
                models += newModelName -> new FastTextWrapper(
                    lr, lrUpdateRate, dim, ws, epoch, minCount, minCountLabel, neg, wordNgrams,
                    lossName, ftModelName, bucket, minn, maxn, thread, t, label, pretrainedFile
                )
                models(modelName).deserialize(newModelName)
            }*/
            
            // Get our model from cache
            val model = FastTextCache.getModel(newModelName)
            
            // Predict
            val prediction = /*models(newModelName)*/model.predict(datum(tokensField) match {
                case a: String => a.split(" ")
                case a: Seq[String] => a
                case a: Any => a.toString.split(" ")
            })
            
            // Append
            datum + (resultName + "_label" -> prediction._1) + (resultName + "_score" -> prediction._2)
        })
    })
}

class FastTextVectorProcessor(resultName: String) extends BaseProcessor(resultName) {
    var modelName: String = _
    var tokensField: String = _
    
    override def initialize(config: JsObject) {
        modelName = (config \ "model_name").as[String]
        tokensField = (config \ "tokens_field").as[String]
    }
    
    override def processor(): Enumeratee[DataPacket, DataPacket] = Enumeratee.mapM((data: DataPacket) => Future {
        new DataPacket(data.data.map {datum =>
            // See if we need to load a new model
            val newModelName = utils.evaluateTuktuString(modelName, datum)
            
            // Get our model from cache
            val model = FastTextCache.getModel(newModelName)
            
            // Predict
            val prediction = model.getSentenceVector(datum(tokensField) match {
                case a: String => a.split(" ")
                case a: Seq[String] => a
                case a: Any => a.toString.split(" ")
            })
            
            // Append
            datum + (resultName -> prediction.toSeq)
        })
    })
}

class SimpleFastTextClassifierProcessor(resultName: String) extends BaseProcessor(resultName) {
    var tokensField: String = _
    val candidateVectors = collection.mutable.ListBuffer.empty[INDArray]
    var model: FastTextWrapper = _
    var top: Int = _
    var flatten: Boolean = _
    var cutoff: Option[Double] = _
    
    override def initialize(config: JsObject) {
        tokensField = (config \ "tokens_field").as[String]
        model = FastTextCache.getModel((config \ "model_name").as[String])
        top = (config \ "top").asOpt[Int].getOrElse(1)
        flatten = (config \ "flatten").asOpt[Boolean].getOrElse(true)
        cutoff = (config \ "cutoff").asOpt[Double]
        
        (config \ "candidates").as[List[List[String]]].foreach {candidateSet =>
            // Compute the vectors for each candidate
            val matrix = Nd4j.create(candidateSet.size, model.getArgs.dim)
            candidateSet.zipWithIndex.foreach {candidate =>
                matrix.putRow(candidate._2, Nd4j.create(model.getWordVector(candidate._1)))
            }
            // Average them to a single vector
            candidateVectors += matrix.mean(0)
        }
    }
    
    override def processor(): Enumeratee[DataPacket, DataPacket] = Enumeratee.mapM((data: DataPacket) => Future {
        new DataPacket(data.data.map {datum =>
            // Apply the sentence vector induction
            val sentenceVector = model.getSentenceVector(datum(tokensField) match {
                case a: String => a.split(" ")
                case a: Seq[String] => a
                case a: Any => a.toString.split(" ")
            })
            
            // Compute cosine similarity to all the candidate vectors and sort by best scoring
            val scores = (candidateVectors.zipWithIndex.map {candidateVector =>
                (candidateVector._2, Transforms.cosineSim(Nd4j.create(sentenceVector), candidateVector._1))
            }).sortWith((a,b) => a._2 > b._2)
            
            // Cutoff
            val cutoffScores = (cutoff match {
                case Some(c) => {
                    // Get only those labels that have a score higher or equal to the cutoff
                    scores.filter(_._2 >= c)
                }
                case None => scores
            }) toList
            
            // Append
            datum + (resultName -> {
                // Flatten if we have to
                if (flatten) scores.head._1 else scores.take(top)
            })
        })
    })
}

/**
 * This classifier is similar to the one above but instead of looking at averaged word vectors, it looks at vectors word-by-word
 * and sees if there is a close-enough overlap between one or more candidate set words and the sentence's words.
 */
class FastTextWordBasedClassifierProcessor(resultName: String) extends BaseProcessor(resultName) {
    var field: String = _
    val candidateVectors = collection.mutable.ListBuffer.empty[List[Array[Double]]]
    var modelName: String = _
    var top: Int = _
    var flatten: Boolean = _
    var cutoff: String = _
    var candidates: List[List[String]] = _
    var candidateField: Option[String] = _

    override def initialize(config: JsObject) {
        field = (config \ "data_field").as[String]
        candidates = (config \ "candidates").as[List[List[String]]].map(_.map(_.toLowerCase))
        top = (config \ "top").asOpt[Int].getOrElse(1)
        flatten = (config \ "flatten").asOpt[Boolean].getOrElse(true)
        cutoff = (config \ "cutoff").asOpt[String].getOrElse("0.7")
        modelName = (config \ "model_name").as[String]
        
        // Overwrites candidate vectors at runtime if given
        candidateField = (config \ "candidate_field").asOpt[String]
        
        super.initialize(config)
    }
    
    def generateCandidates(wordGroups: Seq[Seq[String]], model: FastTextWrapper) = {
        wordGroups.map {words =>
            words.map {word =>
                model.getWordVector(word)
            }
        }
    }

    override def processor(): Enumeratee[DataPacket, DataPacket] = Enumeratee.mapM((data: DataPacket) => Future {
        new DataPacket(data.data.map {datum =>
            datum + (resultName -> {
                // Initialize model and candidate vectors
                val model = FastTextCache.getModel(utils.evaluateTuktuString(modelName, datum))
                if (candidateVectors.isEmpty) {
                    candidates.foreach {candidateSet =>
                        // Compute the vectors for each candidate
                        candidateVectors += candidateSet.map(model.getWordVector)
                    }
                }
                // Get cutoff
                val newCutoff = utils.evaluateTuktuString(cutoff, datum).toDouble
                // Get candidate vectors
                val newCandidates = candidateField match {
                    case Some(field) => datum(field) match {
                        case f: Array[Array[String]] => generateCandidates(f.toSeq.map(_.toSeq), model)
                        case f: Seq[Seq[String]] => generateCandidates(f, model)
                        case f: JsArray => generateCandidates(f.as[Seq[Seq[String]]], model)
                        case f: String =>
                            // By default we assume JSON
                            generateCandidates(Json.parse(f).as[Seq[Seq[String]]], model)
                        case _ => Nil // Can't continue
                    }
                    case None => candidateVectors.toList
                }
                
                // Check field type
                val scores = (datum(field) match {
                    case dtm: Seq[String] => model.simpleWordOverlapClassifier(dtm.toList, newCandidates, newCutoff)
                    case dtm: String      => model.simpleWordOverlapClassifier(dtm.split(" ").toList, newCandidates, newCutoff)
                    case dtm              => model.simpleWordOverlapClassifier(dtm.toString.split(" ").toList, newCandidates, newCutoff)
                }) map {score =>
                    if (score._2 < newCutoff) (-1, 0.0) else score
                }
                
                // Flatten if we have to
                if (flatten) scores.head._1 else scores.take(top)
            })
        })
    })
}