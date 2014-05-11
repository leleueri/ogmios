package io.ogmios.rest.exception

import java.lang.Exception
import spray.http.StatusCodes._
import org.ogmios.core.bean.Status
import org.ogmios.core.bean.OpFailed
import spray.http.StatusCode
import spray.http.StatusCodes._

sealed class OgmiosException(val status: StatusCode, val opStatus: OpFailed) extends Exception

class NotFoundException(opStatus: OpFailed) extends OgmiosException(NotFound, opStatus)
class ConflictException(opStatus: OpFailed) extends OgmiosException(Conflict, opStatus)
class InternalErrorException(opStatus: OpFailed) extends OgmiosException(InternalServerError, opStatus)