package io.ogmios.exception

import java.lang.Exception
import spray.http.StatusCodes._
import io.ogmios.core.bean.OgmiosStatus
import io.ogmios.core.bean.OpFailed
import spray.http.StatusCode
import spray.http.StatusCodes._

sealed class OgmiosException(val status: StatusCode, val opStatus: OpFailed) extends Exception

class NotFoundException(msg: String) extends OgmiosException(NotFound, new OpFailed(OgmiosStatus.StateNotFound, msg))
class ConflictException(msg: String) extends OgmiosException(Conflict, new OpFailed(OgmiosStatus.StateConflict, msg))
class InternalErrorException(msg: String) extends OgmiosException(InternalServerError, new OpFailed(OgmiosStatus.StateKo, msg))
class InvalidArgumentException(msg: String) extends OgmiosException(BadRequest, new OpFailed(OgmiosStatus.StateKo, msg))