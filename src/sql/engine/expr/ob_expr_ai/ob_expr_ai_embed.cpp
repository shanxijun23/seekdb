/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_embed.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/rc/ob_server_runtime.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase 
{
namespace sql 
{
ObExprAIEmbed::ObExprAIEmbed(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, 
                        T_FUN_SYS_AI_EMBED, 
                        N_AI_EMBED, 
                        MORE_THAN_ZERO,
                        NOT_VALID_FOR_GENERATED_COL, 
                        NOT_ROW_DIMENSION) 
{
}

ObExprAIEmbed::~ObExprAIEmbed() 
{
}

int ObExprAIEmbed::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_stack,
                                     int64_t param_num,
                                     common::ObExprTypeCtx &type_ctx) const 
{
  UNUSED(type_ctx);
  UNUSED(types_stack);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 3 || param_num < 2)) {
    ObString func_name_(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    types_stack[MODEL_IDX].set_calc_type(ObVarcharType);
    types_stack[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    types_stack[CONTENT_IDX].set_calc_type(ObVarcharType);
    types_stack[CONTENT_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    if (param_num == 3) {
      ObObjType dim_type = types_stack[DIM_IDX].get_type();
      if (ob_is_integer_type(dim_type)) {
        types_stack[DIM_IDX].set_calc_type(ObIntType);
        types_stack[DIM_IDX].set_precision(10);
        types_stack[DIM_IDX].set_scale(0);
      } else if (ObNullType == dim_type) {
        // NULL dimension means "not specified", the model default dimension is used.
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
      }
    }
    type.set_varchar();
    type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    type.set_collation_level(CS_LEVEL_COERCIBLE);
  }
  return ret;
}

int ObExprAIEmbed::call_embedding_batch(ObIAllocator &allocator,
                                        const ObString &model_id,
                                        ObArray<ObString> &contents,
                                        const int64_t dimension,
                                        ObArray<ObString> &results)
{
  int ret = OB_SUCCESS;
  ObAIFuncExprInfo *info = nullptr;
  share::ObAiModelEndpointInfo resolved_endpoint;
  query::ObIAiEndpointResolver *endpoint_resolver =
      ::oceanbase::share::server_service<::oceanbase::query::ObIAiEndpointResolver>();
  ObJsonObject *config = nullptr;
  ObJsonInt *dim_json = nullptr;
  if (contents.empty()) {
    // nothing to do
  } else if (model_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (dimension <= 0) {
    // use the model default dimension, no "dimensions" field in the request
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, config))) {
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_int(allocator, dimension, dim_json))) {
  } else if (OB_FAIL(config->add("dimensions", dim_json))) {
  }
  if (OB_FAIL(ret) || contents.empty()) {
  } else if (OB_ISNULL(endpoint_resolver)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, model_id, info))) {
  } else if (OB_FAIL(endpoint_resolver->resolve_by_model_name(model_id, allocator, resolved_endpoint))) {
  } else {
    ObAIFuncModel model(allocator, *info, resolved_endpoint);
    if (OB_FAIL(model.call_dense_embedding_vector_v2(contents, config, results))) {
    } else if (results.count() != contents.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "ai_embed, result count does not match input count");
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) 
{
  INIT_SUCC(ret);
  ObDatum *arg_model_id = nullptr;
  ObDatum *arg_content = nullptr;
  ObDatum *arg_dim = nullptr;
  if (expr.arg_cnt_ == 3 ? OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content, arg_dim))
                         : OB_FAIL(expr.eval_param_value(ctx, arg_model_id, arg_content))) {
  } else if (OB_ISNULL(arg_model_id) || OB_ISNULL(arg_content)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (arg_model_id->is_null() || arg_content->is_null()) {
    // NULL propagation: a NULL input produces a NULL result instead of an error.
    res.set_null();
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObString model_id = arg_model_id->get_string();
    ObString content = arg_content->get_string();
    int64_t dim = 0;
    if (OB_NOT_NULL(arg_dim) && !arg_dim->is_null()) {
      dim = arg_dim->get_int();
      if (dim <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (model_id.empty() || content.empty()) {
      res.set_null();
    } else {
      ObArray<ObString> contents;
      ObArray<ObString> results;
      if (OB_FAIL(contents.push_back(content))) {
      } else if (OB_FAIL(call_embedding_batch(temp_allocator, model_id, contents, dim, results))) {
      } else if (results.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, res, results.at(0)))) {
      }
    }
  }
  return ret;
}

int ObExprAIEmbed::eval_ai_embed_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                       const ObBitVector &skip, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)) {
    // nothing to do
  } else {
    ObBitVector &evaluated_flags = expr.get_evaluated_flags(ctx);
    ObDatum *datums = expr.locate_batch_datums(ctx);
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator());
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(N_AI_EMBED));
    ObDatumVector model_vec;
    ObDatumVector content_vec;
    ObDatumVector dim_vec;
    ObArray<ObString> contents;   // inputs carried by the merged request
    ObArray<int64_t> batch_rows;  // rows carried by the merged request
    ObArray<int64_t> single_rows; // rows whose (model, dimension) differs from the first row
    ObString batch_model;
    int64_t batch_dimension = 0;
    bool got_null = false;
    if (OB_ISNULL(datums)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(expr.eval_batch_param_value(ctx, skip, size, model_vec, content_vec, dim_vec))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      if (skip.contain(i) || evaluated_flags.contain(i)) {
        continue;
      }
      const ObDatum *model_datum = model_vec.at(i);
      const ObDatum *content_datum = content_vec.at(i);
      const ObDatum *dim_datum = (3 == expr.arg_cnt_) ? dim_vec.at(i) : nullptr;
      if (OB_ISNULL(model_datum) || OB_ISNULL(content_datum)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (model_datum->is_null() || content_datum->is_null()
                 || model_datum->get_string().empty() || content_datum->get_string().empty()) {
        // NULL input rows are not sent to the model service, but they keep their
        // position in the result, so the mapping between rows and results is stable.
        datums[i].set_null();
        evaluated_flags.set(i);
        got_null = true;
      } else if (OB_NOT_NULL(dim_datum) && !dim_datum->is_null() && dim_datum->get_int() <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ai_embed, dimension parameter must be a positive integer");
      } else {
        ObString model_id = model_datum->get_string();
        int64_t dim = (OB_NOT_NULL(dim_datum) && !dim_datum->is_null()) ? dim_datum->get_int() : 0;
        bool same_key = batch_rows.empty()
                        || (0 == batch_model.case_compare(model_id) && batch_dimension == dim);
        if (same_key) {
          if (batch_rows.empty()) {
            batch_model = model_id;
            batch_dimension = dim;
          }
          if (OB_FAIL(batch_rows.push_back(i))) {
          } else if (OB_FAIL(contents.push_back(content_datum->get_string()))) {
          }
        } else if (OB_FAIL(single_rows.push_back(i))) {
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!batch_rows.empty()) {
      ObArray<ObString> results;
      if (OB_FAIL(call_embedding_batch(temp_allocator, batch_model, contents, batch_dimension, results))) {
        // Graceful degradation: if the merged request is rejected by the model
        // service, evaluate the rows one by one instead of failing the query.
        if (batch_rows.count() > 1 && batch_rows.count() <= 64) {
          LOG_WARN("ai_embed, merged request failed, fallback to single row requests", K(ret),
                   "row_count", batch_rows.count());
          ret = OB_SUCCESS;
          results.reset();
          for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.count(); ++i) {
            if (OB_FAIL(single_rows.push_back(batch_rows.at(i)))) {
            }
          }
          if (OB_SUCC(ret)) {
            batch_rows.reset();
            contents.reset();
          }
        }
      } else if (results.count() != batch_rows.count()) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObEvalCtx::BatchInfoScopeGuard guard(ctx);
        for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.count(); ++i) {
          const int64_t row = batch_rows.at(i);
          guard.set_batch_idx(row);
          if (OB_FAIL(ObAIFuncUtils::set_string_result(expr, ctx, datums[row], results.at(i)))) {
          } else {
            evaluated_flags.set(row);
          }
        }
      }
    }
    if (OB_SUCC(ret) && !single_rows.empty()) {
      ObEvalCtx::BatchInfoScopeGuard guard(ctx);
      for (int64_t i = 0; OB_SUCC(ret) && i < single_rows.count(); ++i) {
        const int64_t row = single_rows.at(i);
        guard.set_batch_idx(row);
        if (OB_FAIL(eval_ai_embed(expr, ctx, datums[row]))) {
        } else {
          evaluated_flags.set(row);
        }
      }
    }
    if (got_null) {
      expr.get_eval_info(ctx).notnull_ = false;
    }
  }
  return ret;
}

int ObExprAIEmbed::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                           const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const 
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  // TODO: support schema version match in plan cache for ai func
  rt_expr.eval_func_ = ObExprAIEmbed::eval_ai_embed;
  // Rows of one batch are merged into one http request in the batch callback.
  rt_expr.eval_batch_func_ = ObExprAIEmbed::eval_ai_embed_batch;
  return ret;
}

} // namespace sql
} // namespace oceanbase
