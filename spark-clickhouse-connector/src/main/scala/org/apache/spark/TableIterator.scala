package org.apache.spark

import org.apache.spark.util.NextIterator

/** Provides a basic/boilerplate Iterator implementation.
 * Extends private scope for NextIterator from apache.spark. */
abstract class TableIterator[U] extends NextIterator[U] {

}
