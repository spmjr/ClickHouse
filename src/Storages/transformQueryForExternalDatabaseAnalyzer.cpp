#include <Storages/transformQueryForExternalDatabaseAnalyzer.h>

#include <Storages/transformQueryForExternalDatabase.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Interpreters/convertFieldToType.h>

#include <Columns/ColumnConst.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ConstantValue.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/JoinNode.h>


#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

class PrepareForExternalDatabaseVisitor : public InDepthQueryTreeVisitor<PrepareForExternalDatabaseVisitor>
{
public:
    using Base = InDepthQueryTreeVisitor<PrepareForExternalDatabaseVisitor>;
    using Base::Base;

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * constant_node = node->as<ConstantNode>();
        if (constant_node)
        {
            auto result_type = constant_node->getResultType();
            if (isDate(result_type) || isDateTime(result_type) || isDateTime64(result_type))
            {
                /// Use string represntation of constant date and time values
                /// The code is ugly, but I don't know hot to convert artbitrary Field to proper string representation
                /// (maybe we can just consider numbers as unix timestamps?)
                auto result_column = result_type->createColumnConst(1, constant_node->getValue());
                const IColumn & inner_column = assert_cast<const ColumnConst &>(*result_column).getDataColumn();

                WriteBufferFromOwnString out;
                result_type->getDefaultSerialization()->serializeText(inner_column, 0, out, FormatSettings());
                node = std::make_shared<ConstantNode>(std::make_shared<ConstantValue>(out.str(), result_type));
            }
        }
    }
};

class CollectUsedIdentifiers : public ConstInDepthQueryTreeVisitor<CollectUsedIdentifiers>
{
public:
    using Base = ConstInDepthQueryTreeVisitor<CollectUsedIdentifiers>;
    using Base::Base;

    CollectUsedIdentifiers(QueryTreeNodePtr & target_table_)
        : target_table(target_table_)
    {
    }

    void visitImpl(const QueryTreeNodePtr & node)
    {
        const auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        const auto & column_source = column_node->getColumnSourceOrNull();
        if (!column_source)
            return;

        if (column_source->getNodeType() != QueryTreeNodeType::TABLE)
            return;

        if (column_source.get() != target_table.get())
            return;

        // const auto & table_storage_id = column_source->as<TableNode>()->getStorageID();

        // if (table_storage_id.database_name != database || table_storage_id.table_name != table)
        //     return;

        if (used_columns_set.contains(column_node->getColumnName()))
            return;

        /// Deduplicate but preserve order
        used_columns_set.insert(column_node->getColumnName());
    }

    Names getUsedColumns() const
    {
        return Names(used_columns_set.begin(), used_columns_set.end());
    }

    const QueryTreeNodePtr & target_table;

    NameOrderedSet used_columns_set;
};


QueryTreeNodePtr getLeftmostTable(const QueryTreeNodePtr & query_tree)
{
    const auto * query_node = query_tree->as<QueryNode>();
    if (!query_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryNode AST is not a QueryNode, got {}", query_tree->getNodeType());

    auto node = query_node->getJoinTree();
    while (true)
    {
        if (node->getNodeType() == QueryTreeNodeType::TABLE)
            return node;

        if (node->getNodeType() == QueryTreeNodeType::JOIN)
            node = node->as<JoinNode>()->getLeftTableExpression();
        else
            break;
    }
    return nullptr;
}

}


Names getUsedColumnsFromQueryTree(const QueryTreeNodePtr & query_tree)
{
    auto target_table = getLeftmostTable(query_tree);
    if (!target_table)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Cannot create external database request for query '{}'", query_tree->formatASTForErrorMessage());

    CollectUsedIdentifiers visitor(target_table);
    visitor.visit(query_tree);
    return visitor.getUsedColumns();
}

ASTPtr getASTForExternalDatabaseFromQueryTree(const QueryTreeNodePtr & query_tree)
{
    auto new_tree = query_tree->clone();

    PrepareForExternalDatabaseVisitor visitor;
    visitor.visit(new_tree);
    const auto * query_node = new_tree->as<QueryNode>();

    const auto & query_node_ast = query_node->toAST({ .add_cast_for_constants = false, .fully_qualified_identifiers = false });

    const auto * union_ast = query_node_ast->as<ASTSelectWithUnionQuery>();
    if (!union_ast)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "QueryNode AST is not a ASTSelectWithUnionQuery");

    if (union_ast->list_of_selects->children.size() != 1)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "QueryNode AST is not a single ASTSelectQuery, got {}", union_ast->list_of_selects->children.size());

    return union_ast->list_of_selects->children.at(0);
}

}
