package org.apache.flink.quickstart

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AverageSensorReadings {
  def main(args: Array[String]): Unit = {
    // 스트리밍 실행 환경 설정. 이 메서드는 호출하는 문맥에 따라 로컬 또는 원격 실행 환경을 반환한다.
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 이벤트 시간 사용
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 스트림 소스에서 DataStream[SensorReading] 생성
    val sensorData: DataStream[SensorReading] = env
      // SensorSource로 센서 읽기 추가
      .addSource(new SensorSource)
      // 타임스탬프와 워터마크 할당(이벤트 시간에 필수)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val avgTemp: DataStream[SensorReading] = sensorData
      // 인라인 람다 함수로 화씨를 섭씨로 변환
      .map( r=> {
        val celsius = (r.temperature - 32) * (5.0 / 9.0)

        SensorReading(r.id, r.timestamp, celsius)
      })
      // 이벤트를 센서 식별자로 분류
      .keyBy(_.id)
      // 5초 길이의 텀블링 윈도우로 이벤트 그룹 생성
      .timeWindow(Time.seconds(5))
      // 사용자 정의 함수로 평균 온도 계산
      .apply(new TemperatureAverager)

    // 표준 출력으로 결과 스트림 출력
    avgTemp.print()

    // 애플리케이션 실행
    env.execute("Compute average sensor temperature")
  }
}

class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {
  override def apply(
                    sensorId: String,
                    window: TimeWindow,
                    vals: Iterable[SensorReading],
                    out: Collector[SensorReading]
                    ): Unit = {
    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    out.collect(SensorReading(sensorId, window.getEnd, avgTemp))
  }
}
