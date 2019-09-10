/*
 * @author Zander Rossouw, Jimmy Spitz, 
 */

#pragma once

#include "badgerdb_exception.h"
#include "types.h"

namespace badgerdb {

/**
 * @Thrown when trying to use delete from empty btree file
 */
class TreeEmptyException : public BadgerDbException {
 public:
  /**
   * Constructs a empty btree exception
   */
  explicit TreeEmptyException();
};

}
