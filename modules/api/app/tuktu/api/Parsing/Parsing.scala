package tuktu.api.Parsing

import play.api.libs.json._
import scala.util.Try
import tuktu.api.utils.{ fieldParser, nearlyEqual }
import tuktu.api.statistics.StatHelper
import fastparse.WhitespaceApi
import fastparse.all.NoTrace

/**
 * Performs arithmetics over a string representation
 */
object ArithmeticParser {
    // Tree structure
    abstract class DoubleNode
    case class DoubleLeaf(d: Double) extends DoubleNode
    case class FunctionLeaf(function: String, parameter: String) extends DoubleNode
    case class AddNode(base: DoubleNode, children: Seq[(String, DoubleNode)]) extends DoubleNode
    case class MultNode(base: DoubleNode, children: Seq[(String, DoubleNode)]) extends DoubleNode
    case class PowNode(seq: Seq[DoubleNode]) extends DoubleNode
    case class NegateNode(base: DoubleNode) extends DoubleNode

    val White = WhitespaceApi.Wrapper {
        import fastparse.all._
        NoTrace(" ".rep)
    }
    import fastparse.noApi._
    import White._

    // Allow all sorts of numbers, negative and scientific notation
    val number: P[DoubleLeaf] = P(
        // If we have a dot, we don't necessarily need a number before the dot
        ("-".? ~ CharIn('0' to '9').rep ~ "." ~ CharIn('0' to '9').rep(min = 1) |
            // Otherwise, we need a number
            "-".? ~ CharIn('0' to '9').rep(min = 1))
            ~ ("e" ~ "-".? ~ CharIn('0' to '9').rep(min = 1)).?).!
        .map { s => DoubleLeaf(s.toDouble) }
    val parens: P[DoubleNode] = P("-".!.? ~ "(" ~/ addSub ~ ")")
        .map { case (neg, n) => if (neg.isDefined) NegateNode(n) else n }
    val factor: P[DoubleNode] = P(parens | number | functions)

    // List of allowed functions
    val allowedFunctions = List("count", "avg", "median", "sum", "max", "min", "stdev")
    // Function parameter
    val parameter: P[String] = P("\"" ~ ("\\\"" | CharPred(_ != '"')).rep ~ "\"").!
        .map { str => Json.parse(str).as[String] }
    // All Tuktu-defined arithmetic functions
    val functions: P[FunctionLeaf] = P(StringIn(allowedFunctions: _*).! ~/ "(" ~/ (parameter | CharPred(_ != ')').rep.!) ~ ")")
        .map { case (func, param) => FunctionLeaf(func, param) }

    // Operations
    val pow: P[PowNode] = P(factor ~ (CharIn("^") ~/ factor).rep)
        .map { case (base, seq) => PowNode(base +: seq) }
    val divMul: P[MultNode] = P(pow ~ (CharIn("*/").! ~/ pow).rep)
        .map { case (base, seq) => MultNode(base, seq) }
    val addSub: P[AddNode] = P(divMul ~ (CharIn("+-").! ~/ divMul).rep)
        .map { case (base, seq) => AddNode(base, seq) }
    val expr: P[DoubleNode] = P(Start ~/ addSub ~ End)

    // Evaluate the tree
    def eval(d: DoubleNode)(implicit data: List[Map[String, Any]] = Nil): Double = d match {
        case DoubleLeaf(d) => d
        case AddNode(base, ops) =>
            ops.foldLeft(eval(base)) {
                case (acc, (op, current)) => op match {
                    case "+" => acc + eval(current)
                    case "-" => acc - eval(current)
                }
            }
        case MultNode(base, ops) =>
            ops.foldLeft(eval(base)) {
                case (acc, (op, current)) => op match {
                    case "*" => acc * eval(current)
                    case "/" => acc / eval(current)
                }
            }
        case PowNode(seq) =>
            seq.foldRight(1d) {
                case (current, acc) => Math.pow(eval(current), acc)
            }
        case NegateNode(n) => -eval(n)
        case FunctionLeaf(f, field) => f match {
            case "avg" =>
                val (sum, count) = data.foldLeft(0.0, 0) {
                    case ((sum, count), datum) =>
                        val v = fieldParser(datum, field).map { StatHelper.anyToDouble(_) }
                        (
                            sum + v.getOrElse(0.0),
                            count + { if (v.isDefined) 1 else 0 })
                }

                if (count > 0)
                    sum / count
                else
                    0.0
            case "median" =>
                val sortedData = (for (datum <- data; v = fieldParser(datum, field) if v.isDefined) yield StatHelper.anyToDouble(v.get)).sorted

                // Find the mid element
                val n = sortedData.size
                if (n == 0)
                    0.0
                else if (n % 2 == 0) {
                    // Get the two elements and average them
                    val n2 = n / 2
                    val n1 = n2 - 1
                    (sortedData(n1) + sortedData(n2)) / 2
                } else
                    sortedData((n - 1) / 2)
            case "sum" =>
                data.foldLeft(0.0) { (sum, datum) => sum + fieldParser(datum, field).map { StatHelper.anyToDouble(_) }.getOrElse(0.0) }
            case "max" =>
                data.foldLeft(Double.MinValue) { (max, datum) =>
                    val v = fieldParser(datum, field).map { StatHelper.anyToDouble(_) }.getOrElse(Double.MinValue)
                    if (v > max) v else max
                }
            case "min" =>
                data.foldLeft(Double.MaxValue) { (min, datum) =>
                    val v = fieldParser(datum, field).map { StatHelper.anyToDouble(_) }.getOrElse(Double.MaxValue)
                    if (v < min) v else min
                }
            case "stdev" =>
                // Get variance
                val vars = StatHelper.getVariances(data, List(field))

                // Sqrt them to get StDevs
                vars.map(v => v._1 -> math.sqrt(v._2)).head._2
            case "count" =>
                data.count { datum => fieldParser(datum, field).isDefined }
        }
    }

    def apply(str: String, data: List[Map[String, Any]] = Nil): Double = eval(expr.parse(str).get.value)(data)
}

/**
 * Parses Boolean predicates
 */
object PredicateParser {
    // Tree structure
    abstract class BooleanNode
    case class BooleanLeaf(b: Boolean) extends BooleanNode
    case class ArithmeticLeaf(left: ArithmeticParser.DoubleNode, op: String, right: ArithmeticParser.DoubleNode) extends BooleanNode
    case class FunctionLeaf(function: String, parameter: String) extends BooleanNode
    case class EqualsNode(node1: BooleanNode, operator: String, b2: BooleanNode) extends BooleanNode
    case class AndNode(children: Seq[BooleanNode]) extends BooleanNode
    case class OrNode(children: Seq[BooleanNode]) extends BooleanNode
    case class NegateNode(or: BooleanNode) extends BooleanNode

    val White = WhitespaceApi.Wrapper {
        import fastparse.all._
        NoTrace(" ".rep)
    }
    import fastparse.noApi._
    import White._

    // Boolean literals
    val literal: P[BooleanLeaf] = P("!".rep.! ~ ("true" | "false").!)
        .map { case (neg, pred) => if (neg.size % 2 == 0) BooleanLeaf(pred.toBoolean) else BooleanLeaf(!pred.toBoolean) }

    // Evaluate arithmetic expressions on numbers using the ArithmeticParser
    val arithExpr: P[ArithmeticLeaf] = P(ArithmeticParser.addSub ~ (">=" | "<=" | "==" | "!=" | "<" | ">").! ~ ArithmeticParser.addSub)
        .map { case (left, op, right) => ArithmeticLeaf(left, op, right) }

    // Evaluate string expressions
    val strings: P[String] = ArithmeticParser.parameter | P(
        CharIn(('a' to 'z') ++ ('A' to 'Z') ++ "_-+.,:;/\"'" ++ ('0' to '9')).rep.!)
    val stringExpr: P[BooleanLeaf] = P(strings ~ ("==" | "!=").! ~ strings)
        .map {
            case (left, "==", right) => left == right
            case (left, "!=", right) => left != right
        }.map { BooleanLeaf(_) }

    // Functions
    val allowedFunctions: List[String] = List("containsFields", "isNumeric", "isNull", "isJSON", "containsSubstring", "isEmptyValue", "listSize")
    val functions: P[FunctionLeaf] = P(((StringIn(allowedFunctions: _*).! ~ "(" ~/ CharPred(_ != ')').rep.! ~/ ")")))
        .map { case (function, parameter) => FunctionLeaf(function, parameter) }
    def allowedParameterfreeFunctions: List[String] = List("isEmpty")
    val parameterfreeFunctions: P[FunctionLeaf] = P(StringIn(allowedParameterfreeFunctions: _*).! ~ "(" ~/ ")")
        .map { case function => FunctionLeaf(function, "") }
    val allFunctions: P[BooleanNode] = P("!".rep.! ~ (functions | parameterfreeFunctions))
        .map { case (n, f) => if (n.size % 2 == 0) f else NegateNode(f) }

    // Bringing everything together
    val basePredicate: P[BooleanNode] = (allFunctions | literal | arithExpr | stringExpr)

    val equals: P[BooleanNode] = P(factor ~ (("==" | "!=").! ~ factor).rep(max = 1))
        .map { case (head, tail) => if (tail.isEmpty) head else EqualsNode(head, tail.head._1, tail.head._2) }
    val and: P[AndNode] = P(equals ~ ("&&" ~/ equals).rep)
        .map { case (head, tail) => AndNode(head +: tail) }
    val or: P[OrNode] = P(and ~ ("||" ~/ and).rep)
        .map { case (head, tail) => OrNode(head +: tail) }

    val parens: P[BooleanNode] = P("!".rep.! ~ "(" ~ or ~ ")")
        .map { case (n, or) => if (n.size % 2 == 0) or else NegateNode(or) }
    val factor: P[BooleanNode] = P(parens | basePredicate)

    val expr: P[BooleanNode] = P(Start ~/ or ~ End)

    def apply(str: String, datum: Map[String, Any] = Map.empty): Boolean = {
        def eval(b: BooleanNode): Boolean = b match {
            case BooleanLeaf(b: Boolean)  => b
            case EqualsNode(n1, "==", n2) => eval(n1) == eval(n2)
            case EqualsNode(n1, "!=", n2) => eval(n1) != eval(n2)
            case AndNode(seq)             => seq.forall { eval(_) }
            case OrNode(seq)              => seq.exists { eval(_) }
            case NegateNode(n)            => !eval(n)
            case ArithmeticLeaf(left, op, right) =>
                val l = ArithmeticParser.eval(left)
                val r = ArithmeticParser.eval(right)
                op match {
                    case "<"  => l < r && !nearlyEqual(l, r)
                    case ">"  => l > r && !nearlyEqual(l, r)
                    case "<=" => l < r || nearlyEqual(l, r)
                    case ">=" => l > r || nearlyEqual(l, r)
                    case "==" => nearlyEqual(l, r)
                    case "!=" => !nearlyEqual(l, r)
                }
            case FunctionLeaf(f, param) => f match {
                case "containsFields" => param.split(',').forall { path =>
                    // Get the path and evaluate it against the datum
                    fieldParser(datum, path).isDefined
                }
                case "isNumeric" => Try {
                    StatHelper.anyToDouble(fieldParser(datum, param).get)
                }.isSuccess
                case "isNull" => fieldParser(datum, param) match {
                    case Some(null)   => true
                    case Some(JsNull) => true
                    case _            => false
                }
                case "isJSON" => param.split(',').forall { path =>
                    // Get the path, evaluate it against the datum, and check if it's JSON
                    fieldParser(datum, path).flatMap { res =>
                        if (res.isInstanceOf[JsValue])
                            res.asInstanceOf[JsValue].asOpt[JsValue].map { _ => true }
                        else
                            None
                    }.getOrElse(false)
                }
                case "containsSubstring" =>
                    // Get the actual string and the substring
                    val split = param.split(",")
                    val string = split(0)
                    val substring = split(1)
                    string.contains(substring)
                case "isEmptyValue" => fieldParser(datum, param) match {
                    case None => false
                    case Some(value) => value match {
                        case a: TraversableOnce[_] => a.isEmpty
                        case a: String             => a.isEmpty
                        case a: JsArray            => a.value.isEmpty
                        case a: JsObject           => a.value.isEmpty
                        case a: JsString           => a.value.isEmpty
                        case a: Any                => a.toString.isEmpty
                    }
                }
                case "listSize" =>
                    // Get the operator and the number to check against
                    val (field, operator, check) = {
                        val split = param.split(",")
                        (split(0), split(1), split(2).toInt)
                    }
                    // Get the list
                    fieldParser(datum, field) match {
                        case None => false
                        case Some(value) => value match {
                            case a: Seq[Any] => {
                                // Get the size of the list
                                val size = a.size
                                // Now match it, use simply matching here to avoid initializing another parser
                                operator match {
                                    case "==" => size == check
                                    case ">=" => size >= check
                                    case "<=" => size <= check
                                    case "!=" => size != check
                                    case "<"  => size < check
                                    case ">"  => size > check
                                    case _    => false
                                }
                            }
                            case _ => false
                        }
                    }
                    false
                case "isEmpty" => datum.isEmpty
            }
        }
        eval(expr.parse(str).get.value)
    }
}