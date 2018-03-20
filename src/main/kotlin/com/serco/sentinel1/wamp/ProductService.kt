package com.serco.sentinel1.wamp

import com.serco.sentinel1.wamp.config.WampConfig
import com.serco.sentinel1.wamp.model.ProductRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody
import java.io.BufferedOutputStream
import java.time.Duration
import java.time.LocalDateTime
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream


@RestController
class ProductService {
    @Autowired lateinit var config: WampConfig
    @Autowired lateinit var productRepo: ProductRepository

    @RequestMapping(value = ["/api/export"], method = [RequestMethod.GET], produces = ["application/zip"])
    @ResponseStatus(value = HttpStatus.OK)
    fun export(@RequestParam(value = "from", required = true) start: String,
               @RequestParam(value = "to", required = true) stop: String): ResponseEntity<StreamingResponseBody> {

        val t = StreamingResponseBody {
            val writer = ZipOutputStream(BufferedOutputStream(it))
            writer.putNextEntry(ZipEntry("exrtact.csv"))

            writer.write("Name, start, stop, mission, dtId, orbit, prodType, timeliness, publishedHub, publishedOda\n".toByteArray())

//            productRepo.findAllIn(LocalDateTime.now(), LocalDateTime.now()).forEach { p ->
//                writer.write("${p.name}, ${p.start}, ${p.stop}, ${p.mission}, ${p.dtId}, ${p.orbit}, ${p.prodType}, ${p.timeliness}, ${p.publishedHub}, ${p.publishedOda}\n".toByteArray())
//            }

            writer.closeEntry()
            writer.close()

        }

        return ResponseEntity.accepted()
                .header("content-disposition", "attachment; filename=\"export.zip\"")
                .body(t);
    }


}

fun Duration.pretty(): String {
    val seconds = seconds
    val absSeconds = Math.abs(seconds);
    val positive = String.format(
        "%d:%02d:%02d",
        absSeconds / 3600,
        (absSeconds % 3600) / 60,
        absSeconds % 60)
    return if(seconds < 0) "-" + positive else positive
}

