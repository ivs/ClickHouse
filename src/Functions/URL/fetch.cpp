#include <Columns/ColumnString.h>
#include <Columns/ColumnStringHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
#include <Functions/formatString.h>
#include <IO/WriteHelpers.h>
#include <base/map.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

using namespace GatherUtils;

class FunctionFetch : public IFunction, WithContext
{
public:
    static constexpr auto name = "fetch";

    explicit FunctionFetch(ContextPtr context_) : WithContext(context_) {}

    static FunctionPtr create(ContextPtr context_) { 
        return std::make_shared<FunctionFetch>(context_);
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }


    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 2",
                getName(),
                arguments.size());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IColumn * c0 = arguments[0].column.get();
        const IColumn * c1 = arguments[1].column.get();

        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnString * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);

        auto col_res = ColumnString::create();

        if (c0_string && c1_string)
            concat(StringSource(*c0_string), StringSource(*c1_string), StringSink(*col_res, c0->size()));
        else if (c0_string && c1_const_string)
            concat(StringSource(*c0_string), ConstSource<StringSource>(*c1_const_string), StringSink(*col_res, c0->size()));
        else if (c0_const_string && c1_string)
            concat(ConstSource<StringSource>(*c0_const_string), StringSource(*c1_string), StringSink(*col_res, c0->size()));
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of arguments of function {}", getName());
        }

        StringSource source(*col_res);

        auto result = ColumnString::create();

        while (!source.isEnd())
        {
            auto slice = source.getWhole();
            std::string url(reinterpret_cast<const char *>(slice.data), slice.size);

            try
            {
                Poco::URI uri(url);
                Poco::Net::HTTPBasicCredentials credentials{};
                auto buf = BuilderRWBufferFromHTTP(uri)
                            .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                            .withMethod(Poco::Net::HTTPRequest::HTTP_GET)
                            .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(
                                    getContext()->getSettingsRef(),
                                    getContext()->getServerSettings().keep_alive_timeout))
                            .create(credentials);
  
                std::string response;
                readStringUntilEOF(response, *buf);
                result->insertData(response.data(), response.size());
            }
            catch (...)
            {
                result->insertData("", 0);
            }

            source.next();
        }

        return result;
    }
};

REGISTER_FUNCTION(Fetch)
{
    factory.registerFunction<FunctionFetch>();
}

}
