#pragma once

#include <Analyzer/IQueryTreeNode.h>


namespace DB
{

Names getUsedColumnsFromQueryTree(const QueryTreeNodePtr & query_tree);

ASTPtr getASTForExternalDatabaseFromQueryTree(const QueryTreeNodePtr & query_tree);

}
