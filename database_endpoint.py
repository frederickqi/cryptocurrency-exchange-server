def process_order(order):
    order = Order(sender_pk=order['sender_pk'], receiver_pk=order['receiver_pk'],
                      buy_currency=order['buy_currency'], sell_currency=order['sell_currency'],
                      buy_amount=order['buy_amount'], sell_amount=order['sell_amount'])
    session.add(order)
    session.commit()

    existing = session.query(Order).all()
    for existing_order in existing:
        if existing_order.filled is None and existing_order.buy_currency == order.sell_currency and existing_order.sell_currency == order.buy_currency and existing_order.sell_amount / existing_order.buy_amount >= order.buy_amount/order.sell_amount:
            order.filled = datetime.now()
            existing_order.filled = datetime.now()
            order.counterparty_id = existing_order.id
            existing_order.counterparty_id = order.id
            session.add(order)
            session.add(existing_order)
            session.commit()

            if existing_order.sell_amount < order.buy_amount:
                child_new_order = {}
                child_new_order['creator_id'] = order.id
                child_new_order['receiver_pk'] = order.receiver_pk
                child_new_order['sender_pk'] = order.sender_pk
                child_new_order['buy_amount'] = order.buy_amount - existing_order.sell_amount
                child_new_order['sell_amount'] = child_new_order['buy_amount']/(order.buy_amount/order.sell_amount)
                child_new_order['sell_currency'] = order.sell_currency
                child_new_order['buy_currency'] = order.buy_currency
                new_order = Order(
                                  creator_id = child_new_order['creator_id'],
                                  receiver_pk = child_new_order['receiver_pk'],
                                  sender_pk= child_new_order['sender_pk'],
                                  buy_amount=child_new_order['buy_amount'],
                                  sell_amount=child_new_order['sell_amount'],
                                  sell_currency= child_new_order['sell_currency'],
                                  buy_currency =child_new_order['buy_currency'])

                session.add(order)
                session.add(existing_order)
                session.add(new_order)
                session.commit()
            elif order.sell_amount < existing_order.buy_amount:
                child_new_order = {}
                child_new_order['creator_id'] = existing_order.id
                child_new_order['receiver_pk'] = existing_order.receiver_pk
                child_new_order['sender_pk'] = existing_order.sender_pk
                child_new_order['buy_amount'] = existing_order.buy_amount - order.sell_amount
                child_new_order['sell_amount'] = child_new_order['buy_amount'] / (existing_order.buy_amount / existing_order.sell_amount)
                child_new_order['sell_currency'] = existing_order.sell_currency
                child_new_order['buy_currency'] = existing_order.buy_currency
                new_order = Order(
                                  creator_id=child_new_order['creator_id'],
                                  receiver_pk=child_new_order['receiver_pk'],
                                  sender_pk=child_new_order['sender_pk'],
                                  buy_amount=child_new_order['buy_amount'],
                                  sell_amount=child_new_order['sell_amount'],
                                  sell_currency=child_new_order['sell_currency'],
                                  buy_currency=child_new_order['buy_currency'])
                session.add(order)
                session.add(existing_order)
                session.add(new_order)
                session.commit()
            break
        else:
            pass
