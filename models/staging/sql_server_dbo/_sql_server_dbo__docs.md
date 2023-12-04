{% docs event_type %}

Operation type carried out by the customer identifier. One of the following values: 

| type            | definition                                 |
|-----------------|--------------------------------------------|
| add_to_cart     | A customer added a product to his/her cart |
| checkout        | A customer checked out his/her order       |
| package_shipped | The order was sent to the customer         |
| page_view       | A customer viewed a product                |

{% enddocs %}

{% docs order_status %}
Order status related to the order identifier. One of the following values: 

| status    | definition                                           |
|-----------|------------------------------------------------------|
| delivered | The order has been received by customer              |
| preparing | The order has been checked out, not yet been shipped |
| shipped   | The order has been shipped, not yet been delivere    |
| no status | The order does not exist                             |

{% enddocs %}

{% docs promotion_status %}
Promotion status related to promotion identifier. Operation type carried out by the customer identifier. One of the following values: 

| status   | definition                    |
|----------|-------------------------------|
| active   | The promotion is ready to use |
| inactive | The promotion has expired     |

{% enddocs %}