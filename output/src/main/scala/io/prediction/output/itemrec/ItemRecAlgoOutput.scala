package io.prediction.output.itemrec

import io.prediction.commons.Config
import io.prediction.commons.modeldata.ItemRecScore
import io.prediction.commons.settings.{ Algo, App, Engine, OfflineEval }

import scala.util.Random

trait ItemRecAlgoOutput {
  /** output the Seq of iids */
  def output(uid: String, n: Int, itypes: Option[Seq[String]])(implicit app: App, algo: Algo, offlineEval: Option[OfflineEval]): Iterator[String]
}

object ItemRecAlgoOutput {
  val config = new Config
  val items = config.getAppdataItems

  def output(uid: String, n: Int, itypes: Option[Seq[String]], latlng: Option[Tuple2[Double, Double]], within: Option[Double], unit: Option[String])(implicit app: App, engine: Engine, algo: Algo, offlineEval: Option[OfflineEval] = None): Seq[String] = {
    /** Serendipity settings. */
    val serendipity = engine.params.get("serendipity").map { _.asInstanceOf[Int] }

    /**
     * Serendipity value (s) from 0-10 in engine settings.
     * Implemented as randomly picking items from top n*(s+1) results.
     */
    val finalN = serendipity.map { s => n * (s + 1) }.getOrElse(n)

    /**
     * At the moment, PredictionIO depends only on MongoDB for its model data storage.
     * Since we are still using the legacy longitude-latitude format, the maximum number
     * of documents that can be returned from a query with geospatial constraint is 100.
     * A "manual join" is still feasible with this size.
     */
    val geoItems: Option[Set[String]] = latlng.map { ll =>
      items.getByAppidAndLatlng(app.id, ll, within, unit).map(_.id).toSet
    }

    val iids: Iterator[String] = geoItems.map { nearByItems =>
      //val geoItems = items.getByAppidAndLatlng(app.id, ll, within, unit).map(_.id).toSet
      // use n = 0 to return all available iids for now
      knnitembased.ItemRecKNNItemBasedAlgoOutput.output(uid, 0, itypes).filter { nearByItems(_) }
    }.getOrElse {
      // use n = 0 to return all available iids for now
      knnitembased.ItemRecKNNItemBasedAlgoOutput.output(uid, 0, itypes)
    }

    /** At this point "output" is guaranteed to have n*(s+1) items (seen or unseen) unless model data is exhausted. */
    val output = iids.take(finalN).toList

    /** Serendipity output. */
    val serendipityOutput = serendipity.map { s =>
      if (s > 0)
        Random.shuffle(output).take(n)
      else
        output
    } getOrElse output

    /**
     * Freshness (0 <= f <= 10) is implemented as the ratio of final results being top N results re-sorted by start time.
     * E.g. For f = 4, 40% of the final output will consist of top N results re-sorted by start time.
     */
    val freshness = engine.params.get("freshness") map { _.asInstanceOf[Int] }

    /** Freshness output. */
    /*val finalOutput = freshness map { f =>
      if (f > 0) {
        val freshnessN = scala.math.round(n * f / 10)
        val otherN = n - freshnessN
        val freshnessOutput = items.getRecentByIds(app.id, output).map(_.id)
        val finalFreshnessOutput = freshnessOutput.take(freshnessN)
        val finalFreshnessOutputSet = finalFreshnessOutput.toSet
        finalFreshnessOutput ++ (serendipityOutput filterNot { finalFreshnessOutputSet(_) }).take(otherN)
      } else
        serendipityOutput
    } getOrElse serendipityOutput*/

    /**
     * Take number of fN = (n * f / 10) from real time rec
     * Then put the real time rec in front of rec, and put origin top (n - real time rec.size) as rest
     */
    val finalOutput = freshness.map { f =>
      val freshnessN = scala.math.round(n * f / 10)

      if (freshnessN > 0) {
        val freshnessOutput: Iterator[String] = geoItems.map { nearByItems =>
          // TODO: hardcode to take 3x more data if geo is used the filter nearby items
          knnitembased.RealTimeItemRecKNNItemBasedAlgoOutput.output(uid, n * 3, itypes).filter { nearByItems(_) }
        }.getOrElse {
          knnitembased.RealTimeItemRecKNNItemBasedAlgoOutput.output(uid, freshnessN, itypes)
        }
        // based on actual freshnessOutput, calculate the remaining otherN from the original
        // recommended list
        val finalFreshnessOutput = freshnessOutput.take(freshnessN).toList // convert iterator to list
        val reaminingN = n - finalFreshnessOutput.size
        val finalFreshnessOutputSet = finalFreshnessOutput.toSet
        finalFreshnessOutput ++ (serendipityOutput.filterNot { finalFreshnessOutputSet(_) }).take(reaminingN)
      } else {
        serendipityOutput
      }
    }.getOrElse {
      serendipityOutput
    }

    finalOutput
  }

}
