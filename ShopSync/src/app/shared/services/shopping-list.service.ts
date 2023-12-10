import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, catchError, map, of } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ShoppingListService {
  private apiUrl = 'http://127.0.0.1:5000/shopping_list/';

  constructor(private http: HttpClient) {}

  getShoppingLists(): Observable<any> {
    return this.http.get<any>(this.apiUrl);
  }

  getShoppingList(id: string): Observable<any> {
    return this.http.get<any>(this.apiUrl + id);
  }

  getListNames(): Observable<any> {
    return this.getShoppingLists();
  }

  updateShoppingListItem(shoppingListUuid: string, itemName: string, itemQuantity: number): Observable<any> {
    const data = {
      shopping_list_uuid: shoppingListUuid,
      item_name: itemName,
      item_quantity: itemQuantity
    };
  
    return this.http.post<any>(`${this.apiUrl}item/change`, data);
  }

  addShoppingListItem(shoppingListUuid: string, itemName: string, itemQuantity: number): Observable<any> {
    const data = {
      shopping_list_uuid: shoppingListUuid,
      item_name: itemName,
      item_quantity: itemQuantity
    };
  
    return this.http.post<any>(`${this.apiUrl}item/add`, data);
  }

  deleteShoppingListItem(shoppingListUuid: string, itemName: string): Observable<any> {
    const data = {
      shopping_list_uuid: shoppingListUuid,
      item_name: itemName,
    };
  
    return this.http.post<any>(`${this.apiUrl}item/delete`, data);
  }
  
}
