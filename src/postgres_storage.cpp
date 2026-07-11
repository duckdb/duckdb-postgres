#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"

#include "duckdb/main/settings.hpp"
#include "postgres_storage.hpp"
#include "storage/postgres_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/postgres_transaction_manager.hpp"

namespace duckdb {

static vector<string> ExtractSchemas(Value &value) {
	if (value.IsNull()) {
		throw BinderException("Value for \"SCHEMA\" option must not be null");
	}
	switch (value.type().id()) {
	case LogicalTypeId::VARCHAR: {
		vector<string> res;
		const string &name = StringValue::Get(value);
		if (name.empty()) {
			throw BinderException("Value \"SCHEMA\" option must be not empty");
		}
		res.push_back(name);
		return res;
	}
	case LogicalTypeId::LIST: {
		if (ListType::GetChildType(value.type()).id() != LogicalTypeId::VARCHAR) {
			throw BinderException(
			    "Value for \"SCHEMA\" option must be either \"VARCHAR\" or \"VARCHAR[]\", was: \"%s\"",
			    value.type().ToString());
		}
		vector<string> res;
		for (const Value &en : ListValue::GetChildren(value)) {
			if (en.IsNull()) {
				throw BinderException("Values for \"SCHEMA\" option must not be null");
			}
			const string &name = StringValue::Get(en);
			if (name.empty()) {
				throw BinderException("Values \"SCHEMA\" option must be not empty");
			}
			res.push_back(name);
		}
		return res;
	}
	default:
		throw BinderException("Value for `SCHEMA` option must be either \"VARCHAR\" or \"VARCHAR[]\", was: \"%s\"",
		                      value.type().ToString());
	}
}

static unique_ptr<Catalog> PostgresAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &attach_options) {
	auto &config = DBConfig::GetConfig(context);
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Attaching Postgres databases is disabled through configuration");
	}
	string attach_path = info.path;

	string secret_name;
	vector<string> schemas_to_load;
	PostgresIsolationLevel isolation_level = PostgresIsolationLevel::REPEATABLE_READ;
	string secret_storage_table_name;
	bool secret_storage_table_specified_explicitly = false;
	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else if (lower_name == "schema") {
			schemas_to_load = ExtractSchemas(entry.second);
		} else if (lower_name == "isolation_level") {
			auto param = entry.second.ToString();
			auto lparam = StringUtil::Lower(param);
			if (lparam == "read committed") {
				isolation_level = PostgresIsolationLevel::READ_COMMITTED;
			} else if (lparam == "repeatable read") {
				isolation_level = PostgresIsolationLevel::REPEATABLE_READ;
			} else if (lparam == "serializable") {
				isolation_level = PostgresIsolationLevel::SERIALIZABLE;
			} else {
				throw BinderException("Invalid value \"%s\" for isolation_level, expected READ COMMITTED, "
				                      "REPEATABLE READ or SERIALIZABLE",
				                      param);
			}
		} else if (lower_name == "secret_storage_table") {
			secret_storage_table_name = entry.second.ToString();
			secret_storage_table_specified_explicitly = true;
		} else {
			throw BinderException("Unrecognized option for Postgres attach: %s", entry.first);
		}
	}
	SecretStorageTable secret_storage_table(std::move(secret_storage_table_name),
	                                        secret_storage_table_specified_explicitly);
	return make_uniq<PostgresCatalog>(context, db, std::move(attach_path), attach_options.access_mode,
	                                  std::move(schemas_to_load), isolation_level, secret_name,
	                                  std::move(secret_storage_table));
}

static unique_ptr<TransactionManager> PostgresCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog) {
	auto &postgres_catalog = catalog.Cast<PostgresCatalog>();
	return make_uniq<PostgresTransactionManager>(db, postgres_catalog);
}

PostgresStorageExtension::PostgresStorageExtension() {
	attach = PostgresAttach;
	create_transaction_manager = PostgresCreateTransactionManager;
}

} // namespace duckdb
