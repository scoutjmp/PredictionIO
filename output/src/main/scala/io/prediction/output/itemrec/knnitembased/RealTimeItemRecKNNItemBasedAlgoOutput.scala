package io.prediction.output.itemrec.knnitembased

import io.prediction.commons.Config
import io.prediction.commons.modeldata.ItemRecScore
import io.prediction.commons.settings.{ Algo, App, OfflineEval }

object RealTimeItemRecKNNItemBasedAlgoOutput {
  private val config = new Config

  def output(uid: String, n: Int, itypes: Option[Seq[String]])(implicit app: App, algo: Algo, offlineEval: Option[OfflineEval]): Iterator[String] = {
    val realTimeItemRecScores = offlineEval map { _ => config.getModeldataTrainingRealTimeItemRecScores } getOrElse config.getModeldataRealTimeItemRecScores
    realTimeItemRecScores.getTopN(uid, n, itypes, None).map(_.iid).toIterator
  }
}
