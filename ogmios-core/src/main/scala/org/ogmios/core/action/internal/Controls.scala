package org.ogmios.core.action.internal

/**
 * Actor message to test if the given element exists
 */
case class IsAvailable[T](elt: T)
/**
 * Actor message used as response to IsAvailable
 */
case class Exit[T](elt: T)
/**
 * Actor message used as response to IsAvailable
 */
case class Available[T](elt: T)