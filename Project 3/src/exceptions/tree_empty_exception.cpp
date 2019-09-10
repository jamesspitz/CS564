/*
 * @author Zander Rossouw, Jimmy Spitz, 
 */

#include "tree_empty_exception.h"
#include <sstream>
#include <string>

namespace badgerdb {

TreeEmptyException::TreeEmptyException()
    : BadgerDbException("")
{
  std::stringstream ss;
  ss << "Index file is Empty.";
  message_.assign(ss.str());
}

}
