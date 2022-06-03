
package object kafka {

  final val UsersTopic = "Users"
  final val FactUsersTopic = "FactUsers"
  type UserId = String
  type AccountId = Long
  type Product = String
  type OrderId = String

  object Domain{
    case class Users(accountId: AccountId, attributes:Map[String, Any], billing_plan_category:Boolean, billing_plan_id:Int,
                     billing_plan_renewal_type:String, is_mobile:Boolean, rule_key:String, user_id:UserId,
                     variables:Map[String, String], visitor_id:String)


    case class FactUsers(account_id:AccountId, attributes_account_id:Map[String, Any], attributes_account_status:String,
                         attributes_billing_end_date:String, attributes_billing_plan_id:Int, attributes_is_safari:Boolean,
                         attributes_is_windows:Boolean, attributes_last_name:String, attributes_os:String,
                         attributes_user_id:UserId, billing_plan_category:Boolean, billing_plan_id:Int, billing_plan_renewal_type:String,
                         is_mobile:Boolean, rule_key:String, variables_plugin_variable:String, visitor_id:String)
  }
}
