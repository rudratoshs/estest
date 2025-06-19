async def __get_provider_locations(self, company_id: int, npi_ids: list) -> dict:
        """Optimized version with better batching and concurrent processing"""
        loop = asyncio.get_event_loop()
        es_client = self.es
        
        # Ensure unique NPI IDs early
        unique_npi_ids = list(set(npi_ids))
        
        # If no NPIs, return empty
        if not unique_npi_ids:
            return {}
        
        # Use smaller chunks for better parallelization
        def chunked(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i + size]
        
        # Optimized search with search_after for better performance
        async def search_batch(index_name, npi_chunk, source_fields):
            query = {
                "size": 10000,  # Increased batch size
                "query": {"terms": {"npi_id": npi_chunk}},
                "_source": source_fields,
                "sort": [{"_id": "asc"}]  # For consistent ordering
            }
            
            def _search():
                try:
                    response = es_client.search(index=index_name, body=query)
                    return response.get('hits', {}).get('hits', [])
                except Exception as e:
                    print(f"ES Error for {index_name}: {e}")
                    return []
            
            return await loop.run_in_executor(None, _search)
        
        # Process all batches concurrently with smaller chunk size
        chunk_size = 2000  # Reduced from 5000 for better parallelization
        chunks = list(chunked(unique_npi_ids, chunk_size))
        
        # Create all tasks upfront
        trigger_tasks = []
        provider_tasks = []
        
        trigger_index = IQVIAPrognos.index_for_company(company_id)
        provider_index = Provider.index_for_company(company_id)
        
        for chunk in chunks:
            trigger_tasks.append(
                search_batch(trigger_index, chunk, ["npi_id", "city", "state"])
            )
            provider_tasks.append(
                search_batch(provider_index, chunk, ["npi_id", "district"])
            )
        
        # Execute all tasks concurrently
        trigger_results, provider_results = await asyncio.gather(
            asyncio.gather(*trigger_tasks),
            asyncio.gather(*provider_tasks)
        )
        
        # Fast data processing with dict comprehensions
        # Build district lookup first
        npi_to_district = {}
        for provider_batch in provider_results:
            npi_to_district.update({
                doc["_source"]["npi_id"]: doc["_source"].get("district") or "UNASSIGNED"
                for doc in provider_batch
                if "npi_id" in doc["_source"]
            })
        
        # Fill missing NPIs
        for npi in unique_npi_ids:
            if npi not in npi_to_district:
                npi_to_district[npi] = "UNASSIGNED"
        
        # Build final structure efficiently
        districts = defaultdict(lambda: {"name": "", "total": 0, "locations": []})
        
        for trigger_batch in trigger_results:
            for doc in trigger_batch:
                source = doc["_source"]
                npi = source.get("npi_id")
                if not npi:
                    continue
                    
                district = npi_to_district.get(npi, "UNASSIGNED")
                city = source.get("city", "")
                state = source.get("state", "")
                
                entry = districts[district]
                entry["name"] = district
                entry["total"] += 1
                entry["locations"].append({"city": city, "state": state})
        
        return dict(districts)
